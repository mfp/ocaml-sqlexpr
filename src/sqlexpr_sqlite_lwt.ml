open Printf
open Sqlexpr_sqlite
open Lwt

module Option = Sqlexpr_utils.Option
module CONC = Sqlexpr_concurrency.Lwt

let failwithfmt fmt = ksprintf (fun s -> try_lwt failwith s) fmt

(* Total number of threads currently running: *)
let thread_count = ref 0

(* Max allowed number of threads *)
let max_threads = ref 4
let set_max_threads n = max_threads := max n !thread_count; !max_threads

module POOL =
struct
  include Sqlexpr_sqlite.Profile(CONC)

  module WT = Weak.Make(struct
                          type t = Stmt.t
                          let hash = Hashtbl.hash
                          let equal = (==)
                        end)
  module rec Ty :
  sig
    type db = {
      id : int;
      file : string;
      mutable db_finished : bool;
      mutable max_workers : int;
      mutable worker_count : int;
      init_func : Sqlite3.db -> unit;
      mutable workers : worker list;
      free_workers : WSet.t;
      db_waiters : worker Lwt.u Lwt_sequence.t;
      tx_key : unit Lwt.key;
    }

    and thread = {
      mutable thread : Thread.t;
      task_channel : (int * (unit -> unit)) Event.channel;
      mutex : Lwt_mutex.t;
    }

    and worker =
    {
      worker_id : int;
      mutable handle : Sqlite3.db;
      stmts : WT.t;
      stmt_cache : Stmt_cache.t;
      worker_thread : thread;
      db : db;
    }
  end = Ty

  and WSet : sig
    type t
    val create : unit -> t
    val is_empty : t -> bool
    val add : t -> Ty.worker -> unit
    val take : t -> Ty.worker
    val remove : t -> Ty.worker -> unit
  end = struct
    module S =
      Set.Make(struct
                 type t = Ty.worker
                 let compare w1 w2 = w1.Ty.worker_id - w2.Ty.worker_id
               end)
    type t = S.t ref

    let create () = ref S.empty
    let is_empty t = S.is_empty !t
    let add t x = t := S.add x !t
    let remove t x = t := S.remove x !t

    let take t =
      let x = S.min_elt !t in
        remove t x;
        x
  end

  include Ty

  type stmt = worker * Stmt.t
  type 'a result = 'a Lwt.t

  module TLS = Lwt

  (* Pool of threads: *)
  let threads : thread Queue.t = Queue.create ()

  (* Queue of clients waiting for a thread to be available: *)
  let waiters : thread Lwt.u Lwt_sequence.t = Lwt_sequence.create ()

  (* will be set to [detach] later, done this way to avoid cumbersome gigantic
   * let rec definition *)
  let do_detach = ref (fun _ _ _ -> return ())

  let rec close_db db =
    db.db_finished <- true;
    List.iter close_worker db.workers

  and close_worker w =
    Stmt_cache.flush_stmts w.stmt_cache;
    (* must run Stmt.finalize and Sqlite3.db_close in the same thread where
     * the handles were created! *)
    ignore begin try_lwt
      !do_detach w
        (fun handle () ->
           WT.iter (fun stmt -> Stmt.finalize stmt) w.stmts;
           ignore (Sqlite3.db_close handle))
        ()
    with e -> return () (* FIXME: log? *)
    end

  let new_id =
    let n = ref 0 in
      fun () -> incr n; !n

  let transaction_key db = db.tx_key

  let open_db ?(init = fun _ -> ()) file =
    let id = new_id () in
    let r =
      {
        id = id; file = file; init_func = init; max_workers = !max_threads;
        worker_count = 0; workers = [];
        free_workers = WSet.create ();
        db_waiters = Lwt_sequence.create ();
        db_finished = false;
        tx_key = Lwt.new_key ();
      }
    in
      Lwt_gc.finalise (fun db -> close_db db; return ()) r;
      r

  let rec thread_loop thread =
    let id, task = Event.sync (Event.receive thread.task_channel) in
      task ();
      Lwt_unix.send_notification id;
      thread_loop thread

  let make_thread () =
    let t =
      {
        thread = Thread.self ();
        task_channel = Event.new_channel ();
        mutex = Lwt_mutex.create ();
      } in
      t.thread <- Thread.create thread_loop t;
      incr thread_count;
      t

  let check_worker_finished worker =
    if worker.db.db_finished then
      failwith (sprintf "db %d:%S is closed" worker.db.id worker.db.file)

  let detach worker f args =
    let result = ref `Nothing in
    let task dbh () =
      try
        result := `Success (f dbh args)
      with exn ->
        result := `Failure exn in
    let waiter, wakener = wait () in
    let id =
      Lwt_unix.make_notification ~once:true
        (fun () ->
           match !result with
             | `Nothing ->
                 wakeup_exn wakener (Failure "Sqlexpr_sqlite.detach")
             | `Success value ->
                 wakeup wakener value
             | `Failure exn ->
                 wakeup_exn wakener exn)
    in
      try_lwt
        WSet.remove worker.db.free_workers worker;
        Lwt_mutex.with_lock worker.worker_thread.mutex
          (fun () ->
             try_lwt
               check_worker_finished worker;
               (* Send the id and the task to the worker: *)
               Event.sync
                 (Event.send worker.worker_thread.task_channel
                    (id, (task worker.handle)));
               return ()
             with e -> wakeup_exn wakener e; return ()) >>
        waiter
      finally
        WSet.add worker.db.free_workers worker;
        return ()

  let () = do_detach := detach

  (* Add a thread to the pool: *)
  let add_thread thread =
    match Lwt_sequence.take_opt_l waiters with
      | None -> Queue.add thread threads
      | Some t -> wakeup t thread

  (* Add a worker to the pool: *)
  let add_worker db worker =
    match Lwt_sequence.take_opt_l db.db_waiters with
      | None -> WSet.add db.free_workers worker
      | Some w -> wakeup w worker

  (* Wait for thread to be available, then return it: *)
  let rec get_thread () =
    if not (Queue.is_empty threads) then
      return (Queue.take threads)
    else if !thread_count < !max_threads then
      return (make_thread ())
    else begin
      let (res, w) = Lwt.task () in
      let node = Lwt_sequence.add_r w waiters in
      Lwt.on_cancel res (fun _ -> Lwt_sequence.remove node);
      res
    end

  let make_worker db =
    db.worker_count <- db.worker_count + 1;
    lwt thread = get_thread () in
      try_lwt
        let worker =
          {
            db = db;
            worker_id = new_id ();
            handle = Sqlite3.db_open ":memory:";
            stmts = WT.create 13;
            stmt_cache = Stmt_cache.create ();
            worker_thread = thread;
          } in
        lwt handle =
          detach worker
            (fun _ () ->
               let handle = Sqlite3.db_open db.file in
                 db.init_func handle;
                 handle)
            ()
        in worker.handle <- handle;
           db.workers <- worker :: db.workers;
           add_worker db worker;
           return worker
      with e ->
        db.worker_count <- db.worker_count - 1;
        raise_lwt e
      finally
        add_thread thread;
        return ()

  let do_raise_error ?sql ?params ?errmsg errcode =
    let msg = Sqlite3.Rc.to_string errcode ^ Option.map_default ((^) " ") "" errmsg in
    let msg = match sql with
        None -> msg
      | Some sql -> sprintf "%s in %s" msg (prettify_sql_stmt sql) in
    let msg = match params with
        None | Some [] -> msg
      | Some params ->
          sprintf "%s with params %s" msg (string_of_params (List.rev params))
    in raise (Error (msg, Sqlite_error (msg, errcode)))

  let raise_error worker ?sql ?params ?errmsg errcode =
    lwt errmsg = match errmsg with
        Some e -> return e
      | None -> detach worker (fun dbh () -> Sqlite3.errmsg dbh) ()
    in try_lwt return (do_raise_error ?sql ?params ~errmsg errcode)

  let rec run ?(retry_on_busy = false) ?stmt ?sql ?params worker f x =
    detach worker f x >>= function
      Sqlite3.Rc.OK | Sqlite3.Rc.ROW | Sqlite3.Rc.DONE as r -> return r
    | Sqlite3.Rc.BUSY when retry_on_busy ->
        Lwt_unix.sleep 0.010 >> run ~retry_on_busy ?sql ?stmt ?params worker f x
    | code ->
        lwt errmsg = detach worker (fun dbh () -> Sqlite3.errmsg dbh) () in
        begin match stmt with
            None -> return ()
          | Some stmt -> detach worker (fun dbh -> Stmt.reset) stmt >> return ()
        end >>
        raise_error worker ?sql ?params ~errmsg code

  let check_ok ?retry_on_busy ?stmt ?sql ?params worker f x =
    lwt _ = run ?retry_on_busy ?stmt ?sql ?params worker f x in return ()

  (* Wait for worker to be available, then return it: *)
  let rec get_worker db =
    if not (WSet.is_empty db.free_workers) then
      return (WSet.take db.free_workers)
    else if db.worker_count < db.max_workers then
      make_worker db
    else begin
      let (res, w) = Lwt.task () in
      let node = Lwt_sequence.add_r w db.db_waiters in
      Lwt.on_cancel res (fun _ -> Lwt_sequence.remove node);
      res
    end

  let prepare db f (params, nparams, sql, stmt_id) =
    lwt worker = get_worker db in
    (try_lwt return (check_worker_finished worker)) >>
    lwt stmt =
      try_lwt
        match stmt_id with
            None ->
              profile_prepare_stmt sql
                (fun () ->
                   lwt stmt = detach worker Stmt.prepare sql in
                     WT.add worker.stmts stmt;
                     return stmt)
          | Some id ->
              match Stmt_cache.find_remove_stmt worker.stmt_cache id with
                Some stmt ->
                  begin try_lwt
                    check_ok ~stmt worker (fun _ -> Stmt.reset) stmt
                  with e ->
                    (* drop the stmt *)
                    detach worker (fun _ -> Stmt.finalize) stmt >>
                    raise_lwt e
                  end >>
                  return stmt
              | None ->
                  profile_prepare_stmt sql
                    (fun () ->
                       lwt stmt = detach worker Stmt.prepare sql in
                         WT.add worker.stmts stmt;
                         return stmt)
      with e ->
        add_worker db worker;
        let s = sprintf "Error with SQL statement %s" sql in
          raise_lwt (Error (s, e))
    in
      (* the list of params is reversed *)
      begin try_lwt
        detach worker
          (fun dbh stmt ->
             let n = ref nparams in
               List.iter
                 (fun v -> match Stmt.bind stmt !n v with
                      Sqlite3.Rc.OK -> decr n
                    | code -> do_raise_error ~sql ~params code)
                 params)
          stmt
        finally
          add_worker db worker;
          return ()
      end >>
      profile_execute_sql sql ~params
        (fun () ->
           try_lwt
             f (worker, stmt) sql params
           finally
             match stmt_id with
                 Some id -> Stmt_cache.add_stmt worker.stmt_cache id stmt; return ()
               | None -> return ())

  let borrow_worker db f =
    let db' =
      { open_db ~init:db.init_func db.file with max_workers = 1;
                                                tx_key = db.tx_key;
      } in
    lwt worker = get_worker db in
      add_worker db' { worker with db = db' } ;
      add_worker db worker;
      try_lwt
        f db'
      finally
        db'.workers <- [];
        close_db db';
        return ()

  let steal_worker db f =
    let db' =
      { open_db ~init:db.init_func db.file with max_workers = 1;
                                                tx_key = db.tx_key;
      } in
    lwt worker = get_worker db in
      add_worker db' { worker with db = db' } ;
      try_lwt
        f db'
      finally
        db'.workers <- [];
        close_db db';
        add_worker db worker;
        return ()

  let step ?sql ?params (worker, stmt) =
    run ?sql ?params ~stmt worker (fun _ -> Stmt.step) stmt

  let step_with_last_insert_rowid ?sql ?params ((worker, _) as stmt) =
    step ?sql ?params stmt >>
    detach worker (fun dbh () -> Sqlite3.last_insert_rowid dbh) ()

  let reset_with_errcode (worker, stmt) =
    detach worker (fun _ -> Stmt.reset) stmt

  let reset x = reset_with_errcode x >> return ()

  let row_data (worker, stmt) = detach worker (fun _ -> Stmt.row_data) stmt

  let unsafe_execute db ?retry_on_busy sql =
    lwt worker = get_worker db in
      try_lwt
        check_ok ?retry_on_busy ~sql worker (fun dbh sql -> Sqlite3.exec dbh sql) sql
      finally
        add_worker db worker;
        return ()

  let raise_error (worker, _) ?sql ?params ?errmsg errcode =
    raise_error worker ?sql ?params ?errmsg errcode
end

include Sqlexpr_sqlite.Make_gen(CONC)(POOL)

