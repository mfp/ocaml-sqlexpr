
open Printf
open ExtList

exception Error of exn
exception Sqlite_error of string * Sqlite3.Rc.t

let raise_thread_error () =
  raise (Error (Failure "Trying to run Sqlite3 function in different thread \
                         than the one where the db was created."))

let curr_thread_id () = Thread.id (Thread.self ())

module Stmt =
struct
  type t = { thread_id : int; handle : Sqlite3.stmt }

  let check_thread t =
    if curr_thread_id () <> t.thread_id then raise_thread_error ()

  let wrap f t = check_thread t; f t.handle

  let prepare dbhandle sql =
    { thread_id = curr_thread_id (); handle = Sqlite3.prepare dbhandle sql; }

  let finalize = wrap Sqlite3.finalize
  let reset = wrap Sqlite3.reset
  let step = wrap Sqlite3.step
  let bind t n v = check_thread t; Sqlite3.bind t.handle n v
  let row_data = wrap Sqlite3.row_data
end

module WT = Weak.Make(struct
                        type t = Stmt.t
                        let hash = Hashtbl.hash
                        let equal = (==)
                      end)

module Types =
struct
  type db =
      {
        handle : Sqlite3.db;
        thread_id : int;
        id : int;
        stmts : WT.t;
      }

  (* (params, nparams, sql, stmt_id)  *)
  type st = (Sqlite3.Data.t list * int * string * string option)
end

include Types

let () =
  Printexc.register_printer
    (function
       | Error exn ->
           Some (sprintf "Sqlexpr_sqlite.Error %s"
                   (Printexc.to_string exn))
       | Sqlite_error (s, rc) ->
           Some (sprintf "Sqlexpr_sqlite.Sqlite_error (%S, %s)"
                   s (Sqlite3.Rc.to_string rc))
       | _ -> None)

let new_id =
  let n = ref 0 in
    fun () -> incr n; !n

let handle db =
  if db.thread_id <> curr_thread_id () then raise_thread_error ();
  db.handle

module Stmt_cache =
struct
  module IH = Hashtbl.Make(struct
                             type t = int
                             let hash n = n
                             let equal = (==)
                           end)
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

  let global_stmt_cache = IH.create 13

  let flush_stmts db = IH.remove global_stmt_cache db.id

  let find_remove_stmt db id =
    try
      let h = IH.find global_stmt_cache db.id in
      let r = H.find h id in
        H.remove h id;
        Some r
    with Not_found -> None

  let add_stmt db id stmt =
    let h =
      try
        IH.find global_stmt_cache db.id
      with Not_found ->
        let h = H.create 13 in
          IH.add global_stmt_cache db.id h;
          h
    in H.add h id stmt
end

let close_db db =
  try
    WT.iter
      (fun stmt -> ignore (Stmt.finalize stmt))
      db.stmts;
    Stmt_cache.flush_stmts db;
    ignore (Sqlite3.db_close (handle db))
  with Sqlite3.Error _ -> () (* FIXME: raise? *)

let open_db fname =
  let r =
    {
      handle = Sqlite3.db_open fname; id = new_id (); stmts = WT.create 13;
      thread_id = Thread.id (Thread.self ());
    }
  in Gc.finalise close_db r;
     r

let sqlite_db db = db.handle

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

module Error(M : THREAD) =
struct
  let raise_error db ?sql ?params ?(errmsg = Sqlite3.errmsg (handle db)) errcode =
    let msg = Sqlite3.Rc.to_string errcode ^ " " ^ errmsg in
    let msg = match sql with
        None -> msg
      | Some sql -> sprintf "%s in %s" msg (prettify_sql_stmt sql) in
    let msg = match params with
        None | Some [] -> msg
      | Some params ->
          sprintf "%s with params %s" msg (string_of_params (List.rev params))
    in M.fail (Error (Sqlite_error (msg, errcode)))

  let raise_exn exn = M.fail (Error exn)

  let failwithfmt fmt = Printf.ksprintf (fun s -> M.fail (Error (Failure s))) fmt
end

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

let profile_op ?(uuid = profile_uuid) op detail f =
  match profile_ch with
      None -> f ()
    | Some ch ->
        let t0 = Unix.gettimeofday () in
        let ret = try Ret (f ()) with e -> Exn e in
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
               Ret r -> r
             | Exn e -> raise e

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
       in profile_op "connect" detail (fun () -> ()))
    profile_ch

module Make(M : THREAD) =
struct
  module Lwt = M
  open Lwt
  include Error(Lwt)

  module Directives = Directives
  module Conversion = Conversion

  open Directives

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

  type olddb = db
  type db = olddb

  exception Error = Error
  exception Sqlite_error = Sqlite_error

  let open_db = open_db
  let close_db = close_db
  let sqlite_db = sqlite_db

  let rec check_ok ?stmt ?sql ?params db f x = match f x with
      Sqlite3.Rc.OK | Sqlite3.Rc.DONE -> return ()
    | Sqlite3.Rc.BUSY | Sqlite3.Rc.LOCKED ->
        M.sleep 0.010 >> check_ok ?sql ?stmt db f x
    | code ->
        let errmsg = Sqlite3.errmsg (handle db) in
          Option.may (fun stmt -> ignore (Stmt.reset stmt)) stmt;
          raise_error db ?sql ?params ~errmsg code

  let maybe_not_found f x = try Some (f x) with Not_found -> None

  let prepare db f (params, nparams, sql, stmt_id) =
    lwt stmt =
      try_lwt
        match stmt_id with
            None ->
              profile_prepare_stmt sql
                (fun () ->
                   let stmt = Stmt.prepare (handle db) sql in
                     WT.add db.stmts stmt;
                     return stmt)
          | Some id ->
              match Stmt_cache.find_remove_stmt db id with
                Some stmt ->
                  begin try_lwt
                    check_ok ~stmt db Stmt.reset stmt
                  with e ->
                    (* drop the stmt *)
                    ignore (Stmt.finalize stmt);
                    fail e
                  end >>
                  return stmt
              | None ->
                  profile_prepare_stmt sql
                    (fun () ->
                       let stmt = Stmt.prepare (handle db) sql in
                         WT.add db.stmts stmt;
                         return stmt)
      with e ->
        failwithfmt "Error with SQL statement %S:\n%s" sql (Printexc.to_string e) in
    let rec iteri ?(i = 0) f = function
        [] -> return ()
      | hd :: tl -> f i hd >> iteri ~i:(i + 1) f tl
    in
      (* the list of params is reversed *)
      iteri
        (fun n v -> check_ok ~sql ~stmt db (Stmt.bind stmt (nparams - n)) v)
        params >>
      profile_execute_sql sql ~params
        (fun () ->
           try_lwt
             f stmt sql params
           finally
             match stmt_id with
                 Some id -> Stmt_cache.add_stmt db id stmt; return ()
               | None -> return ())

  let do_select f db p =
    p.directive (prepare db f) ([], 0, p.sql_statement, p.stmt_id)

  let execute db (p : ('a, unit M.t) statement) =
    do_select (fun stmt sql params ->
                 check_ok ~sql ~params ~stmt db Stmt.step stmt) db p

  let insert db p =
    do_select
      (fun stmt sql params ->
         check_ok ~sql ~params ~stmt db Stmt.step stmt >>
         return (Sqlite3.last_insert_rowid (handle db)))
      db p

  let check_num_cols s stmt expr =
    let expected = fst expr.get_data in
    let actual = Array.length (Stmt.row_data stmt) in
      if expected = actual then return ()
      else
        failwithfmt
          "Sqlexpr_sqlite.%s: wrong number of columns \
           (expected %d, got %d)" s expected actual

  let ensure_reset_stmt stmt f x =
    try_lwt
      f x
    finally
      ignore (Stmt.reset stmt);
      return ()

  let select_f db f expr =
    do_select
      (fun stmt sql params ->
         let auto_yield = M.auto_yield 0.01 in
         let rec loop l =
           auto_yield () >>
           match Stmt.step stmt with
               Sqlite3.Rc.ROW ->
                 check_num_cols "select" stmt expr >>
                 lwt x = try_lwt f (snd expr.get_data (Stmt.row_data stmt)) in
                   loop (x :: l)
             | Sqlite3.Rc.DONE -> return (List.rev l)
             | rc -> raise_error ~sql ~params db rc
         in ensure_reset_stmt stmt loop [])
      db
      expr.statement

  let select db expr = select_f db (fun x -> return x) expr

  let select_one_f_aux db f not_found expr =
    do_select
      (fun stmt sql params ->
         ensure_reset_stmt stmt begin fun () ->
           match Stmt.step stmt with
               Sqlite3.Rc.ROW ->
                 try_lwt f (snd expr.get_data (Stmt.row_data stmt))
             | Sqlite3.Rc.DONE -> not_found ()
             | rc -> raise_error ~sql ~params db rc
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
    ksprintf
      (fun sql -> check_ok ~sql db (Sqlite3.exec (handle db)) sql)
      fmt

  let unsafe_execute_prof text db fmt =
    ksprintf
      (fun sql ->
         profile_prepare_stmt text (fun () -> ());
         profile_execute_sql text (fun () ->
         check_ok ~sql db (Sqlite3.exec (handle db)) sql))
      fmt

  let transaction db f =
    let txid = new_tx_id () in
      unsafe_execute db "SAVEPOINT %s" txid >>
      try_lwt
        lwt x = f db in
          unsafe_execute_prof "RELEASE" db "RELEASE %s" txid >>
          return x
      with e ->
        unsafe_execute_prof "ROLLBACK" db "ROLLBACK TO %s" txid >>
        unsafe_execute db "RELEASE %s" txid >>
        fail e

  let (>>=) = bind

  let fold db f init expr =
    do_select
      (fun stmt sql params ->
         let auto_yield = M.auto_yield 0.01 in
         let rec loop acc =
           auto_yield () >>
           match Stmt.step stmt with
               Sqlite3.Rc.ROW ->
                 begin try_lwt
                   check_num_cols "fold" stmt expr >>
                   f acc (snd expr.get_data (Stmt.row_data stmt))
                 end >>= loop
             | Sqlite3.Rc.DONE -> return acc
             | rc -> raise_error ~sql ~params db rc
         in ensure_reset_stmt stmt loop init)
      db
      expr.statement

  let iter db f expr =
    do_select
      (fun stmt sql params ->
         let auto_yield = M.auto_yield 0.01 in
         let rec loop () =
           auto_yield () >>
           match Stmt.step stmt with
               Sqlite3.Rc.ROW ->
                 begin try_lwt
                   check_num_cols "iter" stmt expr >>
                   f (snd expr.get_data (Stmt.row_data stmt))
                 end >>= loop
             | Sqlite3.Rc.DONE -> return ()
             | rc -> raise_error db ~sql ~params rc
         in ensure_reset_stmt stmt loop ())
      db
      expr.statement
end
