
open Printf

module List = struct include List include BatList end
module Option = BatOption

exception Error of string * exn
exception Sqlite_error of string * Sqlite3.Rc.t

let curr_thread_id () = Thread.id (Thread.self ())

let raise_thread_error ?msg expected =
  let actual = curr_thread_id () in
  let s =
    sprintf
      "Trying to run Sqlite3 function in different thread \
       than the one where the db was created \
       (expected %d, got %d)%s."
      expected
      actual
      (Option.map_default ((^) " ") "" msg)
  in raise (Error (s, (Failure s)))

module Stmt =
struct
  type t = { thread_id : int; dbhandle : Sqlite3.db; handle : Sqlite3.stmt; }

  let check_thread t =
    if curr_thread_id () <> t.thread_id then
      raise_thread_error ~msg:"in Stmt" t.thread_id

  let wrap f t = check_thread t; f t.handle

  let prepare dbhandle sql =
    { thread_id = curr_thread_id (); dbhandle = dbhandle;
      handle = Sqlite3.prepare dbhandle sql; }

  let db_handle t = t.dbhandle

  let finalize t = ignore (wrap Sqlite3.finalize t)
  let reset = wrap Sqlite3.reset
  let step = wrap Sqlite3.step
  let bind t n v = check_thread t; Sqlite3.bind t.handle n v
  let row_data = wrap Sqlite3.row_data
end

module Types =
struct
  (* (params, nparams, sql, stmt_id)  *)
  type st = (Sqlite3.Data.t list * int * string * string option)
end

include Types

let () =
  Printexc.register_printer
    (function
       | Error (s, exn) ->
           Some (sprintf "Sqlexpr_sqlite.Error (%S, %s)" s (Printexc.to_string exn))
       | Sqlite_error (s, rc) ->
           Some (sprintf "Sqlexpr_sqlite.Sqlite_error (%S, %s)"
                   s (Sqlite3.Rc.to_string rc))
       | _ -> None)

let new_id =
  let n = ref 0 in
    fun () -> incr n; !n

module Stmt_cache =
struct
  module H = Hashtbl.Make
               (struct
                  type t = string
                  let hash s =
                    Char.code (String.unsafe_get s 0) +
                    Char.code (String.unsafe_get s 1) lsl 8 +
                    Char.code (String.unsafe_get s 2) lsl 16 +
                    Char.code (String.unsafe_get s 3) lsl 24
                  let equal (s1 : string) s2 = s1 = s2
                end)

  type t = Stmt.t H.t

  let create () = H.create 13

  let flush_stmts t = H.clear t

  let find_remove_stmt t id =
    try
      let r = H.find t id in
        H.remove t id;
        Some r
    with Not_found -> None

  let add_stmt t id stmt = H.add t id stmt
end

module type THREAD = Sqlexpr_concurrency.THREAD

let prettify_sql_stmt sql =
  let b = Buffer.create 80 in
  let last_was_space = ref false in
    for i = 0 to String.length sql - 1 do
      match sql.[i] with
          '\r' | '\n' | '\t' | ' ' ->
             if not !last_was_space then Buffer.add_char b ' ';
             last_was_space := true
        | c -> Buffer.add_char b c;
               last_was_space := false
    done;
    (Buffer.contents b)

let string_of_param = function
    Sqlite3.Data.NONE -> "NONE"
  | Sqlite3.Data.NULL -> "NULL"
  | Sqlite3.Data.INT n -> Int64.to_string n
  | Sqlite3.Data.FLOAT f -> string_of_float f
  | Sqlite3.Data.TEXT s | Sqlite3.Data.BLOB s -> sprintf "%S" s

let string_of_params l = String.concat ", " (List.map string_of_param l)

module Directives =
struct
  module D = Sqlite3.Data

  type ('a, 'b) statement =
      {
        sql_statement : string;
        stmt_id : string option;
        directive : ('a, 'b) directive
      }

  and ('a, 'b) directive = (st -> 'b) -> st -> 'a

  let literal x k st = k st

  let param f k (params, nparams, sql, prep) x =
    k (f x :: params, nparams + 1, sql, prep)

  let int k st n = param (fun n -> D.INT (Int64.of_int n)) k st n

  let int64 k st n = param (fun n -> D.INT n) k st n

  let int32 k st n = param (fun n -> D.INT (Int64.of_int32 n)) k st n

  let text k st s = param (fun s -> D.TEXT s) k st s

  let blob k st s = param (fun s -> D.BLOB s) k st s

  let float k st f = param (fun f -> D.FLOAT f) k st f

  let bool k st b = param (fun b -> D.INT (if b then 1L else 0L)) k st b

  let any k st f x = blob k st (f x)

  let maybe_int k st n =
    param (Option.map_default (fun n -> D.INT (Int64.of_int n)) D.NULL) k st n

  let maybe_int32 k st n =
    param (Option.map_default (fun n -> D.INT (Int64.of_int32 n)) D.NULL) k st n

  let maybe_int64 k st n =
    param (Option.map_default (fun n -> D.INT n) D.NULL) k st n

  let maybe_text k st s =
    param (Option.map_default (fun s -> D.TEXT s) D.NULL) k st s

  let maybe_blob k st s =
    param (Option.map_default (fun s -> D.BLOB s) D.NULL) k st s

  let maybe_float k st f =
    param (Option.map_default (fun f -> D.FLOAT f) D.NULL) k st f

  let maybe_bool k st b =
    param (Option.map_default (fun b -> D.INT (if b then 1L else 0L)) D.NULL) k st b

  let maybe_any k st f x = maybe_blob k st (Option.map f x)
end

module Conversion =
struct
  open Sqlite3.Data

  let failwithfmt fmt = ksprintf failwith fmt

  let error s =
    failwithfmt "Sqlexpr_sqlite error: bad data (expected %s)" s

  let text = function
      TEXT s | BLOB s -> s
    | INT n -> Int64.to_string n
    | FLOAT f -> string_of_float f
    | _ -> error "text"

  let blob = function BLOB s | TEXT s -> s | _ -> error "blob"

  let int = function INT n -> Int64.to_int n | _ -> error "int"
  let int32 = function INT n -> Int64.to_int32 n | _ -> error "int"
  let int64 = function INT n -> n | _ -> error "int"

  let bool = function INT 0L -> false | INT _ -> true | _ -> error "int"

  let float = function
      INT n -> Int64.to_float n
    | FLOAT n -> n
    | _ -> error "float"

  let maybe f = function
      NULL -> None
    | x -> Some (f x)

  let maybe_text = maybe text
  let maybe_blob = maybe blob
  let maybe_int = maybe int
  let maybe_int32 = maybe int32
  let maybe_int64 = maybe int64
  let maybe_float = maybe float
  let maybe_bool = maybe bool
end

type 'a ret = Ret of 'a | Exn of exn

let profile_ch =
  try
    Some (open_out_gen [Open_append; Open_creat; Open_binary] 0o644
            (Unix.getenv "OCAML_SQLEXPR_PROFILE"))
  with Not_found -> None

let profile_uuid =
  let uuid =
    sprintf "%s %d %d %g %s %g"
      (Unix.gethostname ())
      (Unix.getpid ())
      (Unix.getppid ())
      (Unix.gettimeofday ())
      Sys.executable_name
      ((Unix.times ()).Unix.tms_utime)
  in Digest.to_hex (Digest.string uuid)

(* pgocaml_prof wants to see a connect entry *)
let () =
  Option.may
    (fun ch ->
       let detail =
         [
           "user"; "";
           "database"; "";
           "host"; "";
           "port"; "0";
           "prog"; Sys.executable_name
         ]
       in Csv.save_out ch [[ "1"; profile_uuid; "connect"; "0"; "ok" ] @ detail];
          flush ch)
    profile_ch

module Error(M : THREAD) =
struct
  let raise_exn ?(msg="") exn = M.fail (Error (msg, exn))
  let failwithfmt fmt = Printf.ksprintf (fun s -> M.fail (Error (s, Failure s))) fmt
end

module Profile(Lwt : Sqlexpr_concurrency.THREAD) =
struct
  open Lwt
  let profile_op ?(uuid = profile_uuid) op detail f =
    match profile_ch with
        None -> f ()
      | Some ch ->
          let t0 = Unix.gettimeofday () in
          lwt ret =
            try_lwt
              lwt y = f () in
                return (Ret y)
            with e -> return (Exn e) in
          let dt = Unix.gettimeofday () -. t0 in
          let elapsed_time_us = int_of_float (1e6 *. dt) in
            (* the format used by PGOcaml *)
          let ret_txt = match ret with
              Ret _ -> "ok"
            | Exn e -> Printexc.to_string e in
          let row =
            [ "1"; uuid; op; string_of_int elapsed_time_us; ret_txt] @
            detail
          in Csv.save_out ch [row];
             flush ch;
             match ret with
                 Ret r -> return r
               | Exn e -> raise_lwt e

  (* accept a reversed list of params *)
  let profile_execute_sql sql ?(params = []) f =
    match profile_ch with
        None -> f ()
      | Some ch ->
          let details =
            [ "name"; Digest.to_hex (Digest.string sql); "portal"; " " ]
          in profile_op "execute" details f

  let profile_prepare_stmt sql f =
    match profile_ch with
        None -> f ()
      | Some ch ->
          let details =
            [ "query"; sql; "name"; Digest.to_hex (Digest.string sql) ]
          in profile_op "prepare" details f
end

module type POOL =
sig
  type 'a result
  type db
  type stmt
  val open_db : ?init:(Sqlite3.db -> unit) -> string -> db
  val close_db : db -> unit
  val prepare :
    db -> (stmt -> string -> Sqlite3.Data.t list -> 'a result) -> st -> 'a result
  val step :
    ?sql:string -> ?params:Sqlite3.Data.t list -> stmt -> Sqlite3.Rc.t result
  val step_with_last_insert_rowid :
    ?sql:string -> ?params:Sqlite3.Data.t list -> stmt -> Int64.t result
  val reset : stmt -> unit result
  val row_data : stmt -> Sqlite3.Data.t array result
  val raise_error :
    stmt -> ?sql:string -> ?params:Sqlite3.Data.t list -> ?errmsg:string ->
    Sqlite3.Rc.t -> 'a result
  val unsafe_execute : db -> string -> unit result
  val borrow_worker : db -> (db -> 'a result) -> 'a result
  val steal_worker : db -> (db -> 'a result) -> 'a result
end

module WT = Weak.Make(struct
                        type t = Stmt.t
                        let hash = Hashtbl.hash
                        let equal = (==)
                      end)

type single_worker_db =
{
  handle : Sqlite3.db;
  thread_id : int;
  id : int;
  stmts : WT.t;
  stmt_cache : Stmt_cache.t;
}

module IdentityPool(M: THREAD) =
struct
  module Lwt = M
  open Lwt

  include Profile(M)
  include Error(M)

  type db = single_worker_db
  type stmt = Stmt.t
  type 'a result = 'a Lwt.t

  let get_handle db = db.handle

  let handle db =
    if db.thread_id <> curr_thread_id () then
      try_lwt (raise_thread_error ~msg:"in IdentityPool.handle" db.thread_id)
    else return db.handle

  let close_db db =
    try
      WT.iter
        (fun stmt -> Stmt.finalize stmt)
        db.stmts;
      Stmt_cache.flush_stmts db.stmt_cache;
      ignore begin try_lwt
        lwt db = handle db in
          ignore (Sqlite3.db_close db);
          return ()
      with e -> (* FIXME: log? *) return ()
      end
    with Sqlite3.Error _ -> () (* FIXME: raise? *)

  let mutex_tbl = Hashtbl.create 13

  let try_find_mutex db =
    try Some (Hashtbl.find mutex_tbl db.id) with Not_found -> None

  let make handle =
    let id = new_id () in
    let t =
      {
        handle = handle; id = id; stmts = WT.create 13;
        thread_id = Thread.id (Thread.self ());
        stmt_cache = Stmt_cache.create ();
      }
    in
      Hashtbl.add mutex_tbl id (M.create_recursive_mutex ());
      Gc.finalise (fun _ -> Hashtbl.remove mutex_tbl id) t;
      t

  let open_db ?(init = fun _ -> ()) fname =
    let handle = Sqlite3.db_open fname in
      init handle;
      make handle

  let raise_error db ?sql ?params ?(errmsg = Sqlite3.errmsg db) errcode =
    let msg = Sqlite3.Rc.to_string errcode ^ " " ^ errmsg in
    let msg = match sql with
        None -> msg
      | Some sql -> sprintf "%s in %s" msg (prettify_sql_stmt sql) in
    let msg = match params with
        None | Some [] -> msg
      | Some params ->
          sprintf "%s with params %s" msg (string_of_params (List.rev params))
    in M.fail (Error (msg, Sqlite_error (msg, errcode)))

  let rec run ?stmt ?sql ?params db f x = match f x with
      Sqlite3.Rc.OK | Sqlite3.Rc.ROW | Sqlite3.Rc.DONE as r -> return r
    | Sqlite3.Rc.BUSY | Sqlite3.Rc.LOCKED ->
        M.sleep 0.010 >> run ?sql ?stmt ?params db f x
    | code ->
        let errmsg = Sqlite3.errmsg db in
          Option.may (fun stmt -> ignore (Stmt.reset stmt)) stmt;
          raise_error db ?sql ?params ~errmsg code

  let check_ok ?stmt ?sql ?params db f x =
    lwt _ = run ?stmt ?sql ?params db f x in return ()

  let prepare db f (params, nparams, sql, stmt_id) =
    lwt dbh = handle db in
    lwt stmt =
      try_lwt
        match stmt_id with
            None ->
              profile_prepare_stmt sql
                (fun () ->
                   let stmt = Stmt.prepare dbh sql in
                     WT.add db.stmts stmt;
                     return stmt)
          | Some id ->
              match Stmt_cache.find_remove_stmt db.stmt_cache id with
                Some stmt ->
                  begin try_lwt
                    check_ok ~stmt dbh Stmt.reset stmt
                  with e ->
                    (* drop the stmt *)
                    Stmt.finalize stmt;
                    fail e
                  end >>
                  return stmt
              | None ->
                  profile_prepare_stmt sql
                    (fun () ->
                       let stmt = Stmt.prepare dbh sql in
                         WT.add db.stmts stmt;
                         return stmt)
      with e ->
        let msg =
          sprintf "Error with SQL statement %S:\n%s" sql (Printexc.to_string e)
        in raise_exn ~msg e in
    let rec iteri ?(i = 0) f = function
        [] -> return ()
      | hd :: tl -> f i hd >> iteri ~i:(i + 1) f tl
    in
      (* the list of params is reversed *)
      iteri
        (fun n v -> check_ok ~sql ~stmt dbh (Stmt.bind stmt (nparams - n)) v)
        params >>
      profile_execute_sql sql ~params
        (fun () ->
           try_lwt
             f stmt sql params
           finally
             match stmt_id with
                 Some id -> Stmt_cache.add_stmt db.stmt_cache id stmt; return ()
               | None -> return ())

  let borrow_worker db f = f db

  let steal_worker db f =
    match try_find_mutex db with
        None -> (* shouldn't happen as long as the db is alive *)
          failwithfmt
            "Sqlexpr_sqlite (steal_worker): could not find mutex for db %d" db.id
      | Some m ->
          M.with_lock m (fun () -> f db)

  let step ?sql ?params stmt =
    run ?sql ?params ~stmt (Stmt.db_handle stmt) Stmt.step stmt

  let step_with_last_insert_rowid ?sql ?params stmt =
    step ?sql ?params stmt >>
    return (Sqlite3.last_insert_rowid (Stmt.db_handle stmt))

  let reset_with_errcode stmt = return (Stmt.reset stmt)
  let reset stmt = ignore (Stmt.reset stmt); return ()
  let row_data stmt = return (Stmt.row_data stmt)

  let unsafe_execute db sql =
    lwt dbh = handle db in
      check_ok ~sql dbh (Sqlite3.exec dbh) sql

  let raise_error stmt ?sql ?params ?errmsg errcode =
    raise_error (Stmt.db_handle stmt) ?sql ?params ?errmsg errcode
end

module type S =
sig
  type 'a result

  type ('a, 'b) statement =
      {
        sql_statement : string;
        stmt_id : string option;
        directive : (st -> 'b) -> st -> 'a;
      }

  type ('a, 'b, 'c) expression =
      {
        statement : ('a, 'c) statement;
        get_data : int * (Sqlite3.Data.t array -> 'b);
      }

  type db

  exception Error of string * exn
  exception Sqlite_error of string * Sqlite3.Rc.t

  val open_db : ?init:(Sqlite3.db -> unit) -> string -> db
  val close_db : db -> unit
  val borrow_worker : db -> (db -> 'a result) -> 'a result
  val execute : db -> ('a, unit result) statement -> 'a
  val insert : db -> ('a, int64 result) statement -> 'a
  val select : db -> ('c, 'a, 'a list result) expression -> 'c
  val select_f : db -> ('a -> 'b result) -> ('c, 'a, 'b list result) expression -> 'c
  val select_one : db -> ('c, 'a, 'a result) expression -> 'c
  val select_one_maybe : db -> ('c, 'a, 'a option result) expression -> 'c
  val select_one_f : db -> ('a -> 'b result) -> ('c, 'a, 'b result) expression -> 'c
  val select_one_f_maybe : db -> ('a -> 'b result) ->
    ('c, 'a, 'b option result) expression -> 'c
  val transaction : db -> (db -> 'a result) -> 'a result
  val fold :
    db -> ('a -> 'b -> 'a result) -> 'a -> ('c, 'b, 'a result) expression -> 'c
  val iter : db -> ('a -> unit result) -> ('b, 'a, unit result) expression -> 'b

  module Directives :
  sig
    type ('a, 'b) directive = (st -> 'b) -> st -> 'a

    val literal : string -> ('a, 'a) directive
    val int : (int -> 'a, 'a) directive
    val text : (string -> 'a, 'a) directive
    val blob : (string -> 'a, 'a) directive
    val float : (float -> 'a, 'a) directive
    val int32 : (int32 -> 'a, 'a) directive
    val int64 : (int64 -> 'a, 'a) directive
    val bool : (bool -> 'a, 'a) directive
    val any : (('b -> string) -> 'b -> 'a, 'a) directive

    val maybe_int : (int option -> 'a, 'a) directive
    val maybe_text : (string option -> 'a, 'a) directive
    val maybe_blob : (string option -> 'a, 'a) directive
    val maybe_float : (float option -> 'a, 'a) directive
    val maybe_int32 : (int32 option -> 'a, 'a) directive
    val maybe_int64 : (int64 option -> 'a, 'a) directive
    val maybe_bool : (bool option -> 'a, 'a) directive
    val maybe_any : (('b -> string) -> 'b option -> 'a, 'a) directive
  end

  module Conversion :
  sig
    val text : Sqlite3.Data.t -> string
    val blob : Sqlite3.Data.t -> string
    val int : Sqlite3.Data.t -> int
    val int32 : Sqlite3.Data.t -> int32
    val int64 : Sqlite3.Data.t -> int64
    val float : Sqlite3.Data.t -> float
    val bool : Sqlite3.Data.t -> bool
    val maybe : (Sqlite3.Data.t -> 'a) -> Sqlite3.Data.t -> 'a option
    val maybe_text : Sqlite3.Data.t -> string option
    val maybe_blob : Sqlite3.Data.t -> string option
    val maybe_int : Sqlite3.Data.t -> int option
    val maybe_int32 : Sqlite3.Data.t -> int32 option
    val maybe_int64 : Sqlite3.Data.t -> int64 option
    val maybe_float : Sqlite3.Data.t -> float option
    val maybe_bool : Sqlite3.Data.t -> bool option
  end
end

module Make_gen(M : THREAD)(POOL : POOL with type 'a result = 'a M.t) =
struct
  module Lwt = M
  open Lwt
  include Error(M)
  include Profile(M)

  module Directives = Directives
  module Conversion = Conversion

  open Directives

  let (>>=) = bind
  type 'a result = 'a M.t

  type ('a, 'b) statement = ('a, 'b) Directives.statement =
      {
        sql_statement : string;
        stmt_id : string option;
        directive : ('a, 'b) directive
      }

  type ('a, 'b, 'c) expression = {
    statement : ('a, 'c) statement;
    get_data : int * (Sqlite3.Data.t array -> 'b);
  }

  type db = POOL.db

  exception Error = Error
  exception Sqlite_error = Sqlite_error

  let open_db = POOL.open_db
  let close_db = POOL.close_db

  let borrow_worker = POOL.borrow_worker

  let do_select f db p =
    p.directive (POOL.prepare db f) ([], 0, p.sql_statement, p.stmt_id)

  let execute db (p : ('a, _ M.t) statement) =
    do_select
      (fun stmt sql params -> POOL.step ~sql ~params stmt >> return ())
      db p

  let insert db p =
    do_select
      (fun stmt sql params ->
         POOL.step_with_last_insert_rowid ~sql ~params stmt)
      db p

  let check_num_cols s stmt expr data =
    let expected = fst expr.get_data in
    let actual = Array.length data in
      if expected = actual then return ()
      else
        failwithfmt
          "Sqlexpr_sqlite.%s: wrong number of columns \
           (expected %d, got %d) in SQL: %s" s expected actual
          expr.statement.sql_statement

  let ensure_reset_stmt stmt f x =
    try_lwt
      f x
    finally
      POOL.reset stmt

  let select_f db f expr =
    do_select
      (fun stmt sql params ->
         let auto_yield = M.auto_yield 0.01 in
         let rec loop l =
           auto_yield () >>
           POOL.step stmt >>= function
               Sqlite3.Rc.ROW ->
                 lwt data = POOL.row_data stmt in
                 check_num_cols "select" stmt expr data >>
                 lwt x = try_lwt f (snd expr.get_data data) in
                   loop (x :: l)
             | Sqlite3.Rc.DONE -> return (List.rev l)
             | rc -> POOL.raise_error ~sql ~params stmt rc
         in ensure_reset_stmt stmt loop [])
      db
      expr.statement

  let select db expr = select_f db (fun x -> return x) expr

  let select_one_f_aux db f not_found expr =
    do_select
      (fun stmt sql params ->
         ensure_reset_stmt stmt begin fun () ->
           POOL.step stmt >>= function
               Sqlite3.Rc.ROW ->
                 lwt data = POOL.row_data stmt in
                 try_lwt f (snd expr.get_data data)
             | Sqlite3.Rc.DONE -> not_found ()
             | rc -> POOL.raise_error ~sql ~params stmt rc
         end ())
      db
      expr.statement

  let select_one db expr =
    select_one_f_aux db (fun x -> return x) (fun () -> M.fail Not_found) expr

  let select_one_f db f expr =
    select_one_f_aux db f (fun () -> M.fail Not_found) expr

  let select_one_maybe db expr =
    select_one_f_aux db (fun x -> return (Some x)) (fun () -> return None) expr

  let select_one_f_maybe db f expr =
    select_one_f_aux db
      (fun x -> lwt y = f x in return (Some y)) (fun () -> return None) expr

  let new_tx_id =
    let pid = Unix.getpid () in
    let n = ref 0 in
      fun () -> incr n; sprintf "__sqlexpr_sqlite_tx_%d_%d" pid !n

  let unsafe_execute db fmt =
    ksprintf (POOL.unsafe_execute db) fmt

  let unsafe_execute_prof text db fmt =
    ksprintf
      (fun sql ->
         profile_prepare_stmt text (fun () -> return ()) >>
         profile_execute_sql text (fun () -> POOL.unsafe_execute db sql))
      fmt

  let transaction db f =
    let txid = new_tx_id () in
      POOL.steal_worker db
        (fun db ->
           unsafe_execute db "SAVEPOINT %s" txid >>
           try_lwt
             lwt x = f db in
               unsafe_execute_prof "RELEASE" db "RELEASE %s" txid >>
               return x
           with e ->
             unsafe_execute_prof "ROLLBACK" db "ROLLBACK TO %s" txid >>
             unsafe_execute db "RELEASE %s" txid >>
             fail e)

  let fold db f init expr =
    do_select
      (fun stmt sql params ->
         let auto_yield = M.auto_yield 0.01 in
         let rec loop acc =
           auto_yield () >>
           POOL.step stmt >>= function
               Sqlite3.Rc.ROW ->
                 begin try_lwt
                   lwt data = POOL.row_data stmt in
                   check_num_cols "fold" stmt expr data >>
                   f acc (snd expr.get_data data)
                 end >>= loop
             | Sqlite3.Rc.DONE -> return acc
             | rc -> POOL.raise_error ~sql ~params stmt rc
         in ensure_reset_stmt stmt loop init)
      db
      expr.statement

  let iter db f expr =
    do_select
      (fun stmt sql params ->
         let auto_yield = M.auto_yield 0.01 in
         let rec loop () =
           auto_yield () >>
           POOL.step stmt >>= function
               Sqlite3.Rc.ROW ->
                 begin try_lwt
                   lwt data = POOL.row_data stmt in
                   check_num_cols "iter" stmt expr data >>
                   f (snd expr.get_data data)
                 end >>= loop
             | Sqlite3.Rc.DONE -> return ()
             | rc -> POOL.raise_error stmt ~sql ~params rc
         in ensure_reset_stmt stmt loop ())
      db
      expr.statement
end

module Make(M : THREAD) = struct
  module Id = IdentityPool(M)
  include Make_gen(M)(Id)
  let make = Id.make
  let sqlite_db db = Id.get_handle db
end
