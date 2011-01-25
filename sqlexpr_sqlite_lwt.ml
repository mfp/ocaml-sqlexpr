open Printf
open Sqlexpr_sqlite
open Lwt

let failwithfmt fmt = ksprintf (fun s -> try_lwt failwith s) fmt

module POOL =
struct
  module WT = Weak.Make(struct
                          type t = Stmt.t
                          let hash = Hashtbl.hash
                          let equal = (==)
                        end)

  type db = {
    id : int;
    file : string;
    mutable max_threads : int;
    workers : worker Queue.t;
    waiters : worker Lwt.u Lwt_sequence.t;
    mutable thread_count : int;
  }

  and worker =
  {
    mutable handle : Sqlite3.db;
    stmts : WT.t;
    stmt_cache : Stmt_cache.t;
    task_channel : (int * (Sqlite3.db -> unit)) Event.channel;
    mutable thread : Thread.t;
    mutable finished : bool;
    db : db; (* keep ref to db so it isn't GCed while a worker is active *)
  }

  type stmt = worker * Stmt.t
  type 'a result = 'a Lwt.t

  let max_threads = ref 4
  let set_default_max_threads n = max_threads := n

  let close_db db =
    db.max_threads <- 0;
    let e = Error (Failure (sprintf "Handle closed for DB %S" db.file)) in
      Lwt_sequence.iter_l (fun u -> wakeup_exn u e) db.waiters;
      Queue.iter
        (fun worker ->
           if not worker.finished then begin
             let id = Lwt_unix.make_notification ~once:true (fun () -> ()) in
               Event.sync (Event.send worker.task_channel (id, (fun _ -> ())));
           end)
        db.workers

  let new_id =
    let n = ref 0 in
      fun () -> incr n; !n

  let open_db file =
    let id = new_id () in
    let r =
      {
        id; file;
        max_threads = !max_threads;
        waiters = Lwt_sequence.create ();
        workers = Queue.create ();
        thread_count = 0;
      }
    in Gc.finalise close_db r;
       r

  let close_worker w =
    w.finished <- true;
    try
      WT.iter (fun stmt -> Stmt.finalize stmt) w.stmts;
      Stmt_cache.flush_stmts w.stmt_cache;
      ignore (Sqlite3.db_close w.handle)
    with Sqlite3.Error _ -> () (* FIXME: raise? *)

  let worker_loop worker () =
    let rec do_worker_loop () =
      let id, task = Event.sync (Event.receive worker.task_channel) in
      let break = ref false in
        task worker.handle;
        if worker.db.thread_count > worker.db.max_threads then break := true;
        Lwt_unix.send_notification id;
        if not !break then
          do_worker_loop ()
        else begin
          worker.finished <- true;
          close_worker worker
        end
    in
      worker.handle <- Sqlite3.db_open worker.db.file;
      do_worker_loop ()

  let make_worker db =
    db.thread_count <- db.thread_count + 1;
    let worker =
      {
        db = db;
        handle = Sqlite3.db_open ":memory:";
        stmts = WT.create 13;
        stmt_cache = Stmt_cache.create ();
        task_channel = Event.new_channel ();
        thread = Thread.self ();
        finished = false;
      }
    in worker.thread <- Thread.create (worker_loop worker) ();
       worker

  (* Add a worker to the pool: *)
  let add_worker db worker =
    match Lwt_sequence.take_opt_l db.waiters with
      | None -> Queue.add worker db.workers
      | Some w -> wakeup w worker

  (* Wait for worker to be available, then return it: *)
  let rec get_worker db =
    if not (Queue.is_empty db.workers) then
      return (Queue.take db.workers)
    else if db.thread_count < db.max_threads then
      return (make_worker db)
    else begin
      let (res, w) = Lwt.task () in
      let node = Lwt_sequence.add_r w db.waiters in
      Lwt.on_cancel res (fun _ -> Lwt_sequence.remove node);
      res
    end

  let check_worker_finished worker =
    if worker.finished then
      failwith (sprintf "worker %d (db %d:%S) is finished!"
                  (Thread.id worker.thread) worker.db.id worker.db.file)

  let detach worker f args =
    let result = ref `Nothing in
    let task dbh =
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
    in try_lwt
      check_worker_finished worker;
      (* Send the id and the task to the worker: *)
      Event.sync (Event.send worker.task_channel (id, task));
      waiter

  let raise_error worker ?sql ?params ?errmsg errcode =
    lwt errmsg = match errmsg with
        Some e -> return e
      | None -> detach worker (fun dbh () -> Sqlite3.errmsg dbh) () in
    let msg = Sqlite3.Rc.to_string errcode ^ " " ^ errmsg in
    let msg = match sql with
        None -> msg
      | Some sql -> sprintf "%s in %s" msg (prettify_sql_stmt sql) in
    let msg = match params with
        None | Some [] -> msg
      | Some params ->
          sprintf "%s with params %s" msg (string_of_params (List.rev params)) in
      raise_lwt (Error (Sqlite_error (msg, errcode)))

  let rec run ?stmt ?sql ?params worker f x = detach worker f x >>= function
      Sqlite3.Rc.OK | Sqlite3.Rc.ROW | Sqlite3.Rc.DONE as r -> return r
    | Sqlite3.Rc.BUSY | Sqlite3.Rc.LOCKED ->
        Lwt_unix.sleep 0.010 >> run ?sql ?stmt ?params worker f x
    | code ->
        lwt errmsg = detach worker (fun dbh () -> Sqlite3.errmsg dbh) () in
        begin match stmt with
            None -> return ()
          | Some stmt -> detach worker (fun dbh -> Stmt.reset) stmt >> return ()
        end >>
        raise_error worker ?sql ?params ~errmsg code

  let check_ok ?stmt ?sql ?params worker f x =
    lwt _ = run ?stmt ?sql ?params worker f x in return ()

  let rec iteri ?(i = 0) f = function
      [] -> return ()
    | hd :: tl -> f i hd >> iteri ~i:(i + 1) f tl

  let prepare db f (params, nparams, sql, stmt_id) =
    lwt worker = get_worker db in
    (try_lwt return (check_worker_finished worker)) >>
    lwt stmt =
      try_lwt
        match stmt_id with
            None ->
              lwt stmt = detach worker Stmt.prepare sql in
                WT.add worker.stmts stmt;
                return stmt
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
                  lwt stmt = detach worker Stmt.prepare sql in
                    WT.add worker.stmts stmt;
                    return stmt
      with e ->
        add_worker db worker;
        failwithfmt "Error with SQL statement %S:\n%s" sql (Printexc.to_string e)
    in
      (* the list of params is reversed *)
      iteri
        (fun n v -> check_ok ~sql ~stmt worker
                      (fun _ -> (Stmt.bind stmt (nparams - n))) v)
        params >>
      try_lwt
        f (worker, stmt) sql params
      finally
        add_worker db worker;
        match stmt_id with
            Some id -> Stmt_cache.add_stmt worker.stmt_cache id stmt; return ()
          | None -> return ()

  let step ?sql ?params (worker, stmt) =
    run ?sql ?params ~stmt worker (fun _ -> Stmt.step) stmt

  let step_with_last_insert_rowid ?sql ?params ((worker, _) as stmt) =
    step ?sql ?params stmt >>
    detach worker (fun dbh () -> Sqlite3.last_insert_rowid dbh) ()

  let data_count (worker, stmt) =
    detach worker (fun _ -> Stmt.data_count) stmt

  let reset_with_errcode (worker, stmt) =
    detach worker (fun _ -> Stmt.reset) stmt

  let reset x = reset_with_errcode x >> return ()

  let row_data (worker, stmt) = detach worker (fun _ -> Stmt.row_data) stmt

  let unsafe_execute db sql =
    lwt worker = get_worker db in
      try_lwt
        check_ok ~sql worker (fun dbh sql -> Sqlite3.exec dbh sql) sql
      finally
        add_worker db worker;
        return ()

  let raise_error (worker, _) ?sql ?params ?errmsg errcode =
    raise_error worker ?sql ?params ?errmsg errcode
end

module CONC =
struct
  include Lwt
  let auto_yield = Lwt_unix.auto_yield
  let sleep = Lwt_unix.sleep
end

include Sqlexpr_sqlite.Make_gen(CONC)(POOL)

