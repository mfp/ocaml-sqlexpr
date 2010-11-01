
open Printf
open ExtList

exception Error of exn
exception Sqlite_error of string * Sqlite3.Rc.t

module WT = Weak.Make(struct
                        type t = Sqlite3.stmt
                        let hash = Hashtbl.hash
                        let equal = (==)
                      end)

module IH = Hashtbl.Make(struct
                           type t = int
                           let hash n = n
                           let equal = (==)
                         end)

type db =
    {
      db : Sqlite3.db;
      id : int;
      stmts : WT.t;
      mutable stmt_caches : Sqlite3.stmt IH.t list;
    }

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

let open_db fname =
  {
    db = Sqlite3.db_open fname; id = new_id (); stmts = WT.create 13;
    stmt_caches = [];
  }

let close_db db =
  try
    WT.iter
      (fun stmt -> ignore (Sqlite3.finalize stmt))
      db.stmts;
    List.iter (fun cache -> IH.remove cache db.id) db.stmt_caches;
    ignore (Sqlite3.db_close db.db)
  with Sqlite3.Error _ -> () (* FIXME: raise? *)

let sqlite_db db = db.db

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
  let raise_error db ?sql ?params ?(errmsg = Sqlite3.errmsg db.db) errcode =
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

  type st = (Sqlite3.Data.t list * int * string * Sqlite3.stmt IH.t option)

  and ('a, 'b) statement =
      {
        sql_statement : string;
        stmt_cache : Sqlite3.stmt IH.t option;
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

module MkConversion(M : THREAD) =
struct
  open Sqlite3.Data
  module Lwt = M
  open Lwt
  include Error(M)

  let error s =
    failwithfmt "Sqlexpr_sqlite error: bad data (expected %s)" s

  let text = function
      TEXT s | BLOB s -> return s
    | INT n -> return (Int64.to_string n)
    | FLOAT f -> return (string_of_float f)
    | _ -> error "text"

  let blob = function BLOB s | TEXT s -> return s | _ -> error "blob"

  let int = function INT n -> return (Int64.to_int n) | _ -> error "int"
  let int32 = function INT n -> return (Int64.to_int32 n) | _ -> error "int"
  let int64 = function INT n -> return n | _ -> error "int"

  let bool = function INT 0L -> return false | INT _ -> return true | _ -> error "int"

  let float = function
      INT n -> return (Int64.to_float n)
    | FLOAT n -> return n
    | _ -> error "float"

  let maybe f = function
      NULL -> return None
    | x -> lwt y = f x in return (Some y)

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
  module Conversion = MkConversion(M)

  open Directives

  type ('a, 'b) statement = ('a, 'b) Directives.statement

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

  let make_statement ~cacheable sql directive =
    {
      sql_statement = sql;
      stmt_cache = if cacheable then Some (IH.create 13) else None;
      directive = directive;
    }

  let make_expression stmt n f = { statement = stmt; get_data = (n, f) }

  let rec check_ok ?stmt ?sql ?params db f x = match f x with
      Sqlite3.Rc.OK | Sqlite3.Rc.DONE -> return ()
    | Sqlite3.Rc.BUSY | Sqlite3.Rc.LOCKED ->
        M.sleep 0.010 >> check_ok ?sql ?stmt db f x
    | code ->
        let errmsg = Sqlite3.errmsg db.db in
          Option.may (fun stmt -> ignore (Sqlite3.reset stmt)) stmt;
          raise_error db ?sql ?params ~errmsg code

  let maybe_not_found f x = try Some (f x) with Not_found -> None

  let prepare db f (params, nparams, sql, cache) =
    lwt stmt =
      try_lwt
        match cache with
            None ->
              profile_prepare_stmt sql
                (fun () ->
                   let stmt = Sqlite3.prepare db.db sql in
                     WT.add db.stmts stmt;
                     return stmt)
          | Some cache -> match maybe_not_found (IH.find cache) db.id with
                Some stmt -> check_ok ~stmt db Sqlite3.reset stmt >> return stmt
              | None ->
                  profile_prepare_stmt sql
                    (fun () ->
                       let stmt = Sqlite3.prepare db.db sql in
                         IH.add cache db.id stmt;
                         db.stmt_caches <- cache :: db.stmt_caches;
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
        (fun n v -> check_ok ~sql ~stmt db (Sqlite3.bind stmt (nparams - n)) v)
        params >>
      profile_execute_sql sql ~params (fun () -> f stmt sql params)

  let do_select f db p =
    p.directive (prepare db f) ([], 0, p.sql_statement, p.stmt_cache)

  let execute db (p : ('a, unit M.t) statement) =
    do_select (fun stmt sql params ->
                 check_ok ~sql ~params ~stmt db Sqlite3.step stmt) db p

  let insert db p =
    do_select
      (fun stmt sql params ->
         check_ok ~sql ~params ~stmt db Sqlite3.step stmt >>
         return (Sqlite3.last_insert_rowid db.db))
      db p

  let check_num_cols s stmt expr =
    let expected = fst expr.get_data in
    let actual = Array.length (Sqlite3.row_data stmt) in
      if expected = actual then return ()
      else
        failwithfmt
          "Sqlexpr_sqlite.%s: wrong number of columns \
           (expected %d, got %d)" s expected actual

  let ensure_reset_stmt stmt f x =
    try_lwt
      f x
    finally
      ignore (Sqlite3.reset stmt);
      return ()

  let select_f db f expr =
    do_select
      (fun stmt sql params ->
         let auto_yield = M.auto_yield 0.01 in
         let rec loop l =
           auto_yield () >>
           match Sqlite3.step stmt with
               Sqlite3.Rc.ROW ->
                 check_num_cols "select" stmt expr >>
                 lwt x = f (snd expr.get_data (Sqlite3.row_data stmt)) in
                   loop (x :: l)
             | Sqlite3.Rc.DONE -> return (List.rev l)
             | rc -> raise_error ~sql ~params db rc
         in ensure_reset_stmt stmt loop [])
      db
      expr.statement

  let select db expr = select_f db (fun x -> x) expr

  let select_one db expr =
    do_select
      (fun stmt sql params ->
         ensure_reset_stmt stmt begin fun () ->
           match Sqlite3.step stmt with
               Sqlite3.Rc.ROW -> snd expr.get_data (Sqlite3.row_data stmt)
             | Sqlite3.Rc.DONE -> M.fail Not_found
             | rc -> raise_error ~sql ~params db rc
         end ())
      db
      expr.statement

  let new_tx_id =
    let pid = Unix.getpid () in
    let n = ref 0 in
      fun () -> incr n; sprintf "__sqlexpr_sqlite_tx_%d_%d" pid !n

  let unsafe_execute db fmt =
    ksprintf (fun sql -> check_ok ~sql db (Sqlite3.exec db.db) sql) fmt

  let unsafe_execute_prof text db fmt =
    ksprintf
      (fun sql ->
         profile_prepare_stmt text (fun () -> ());
         profile_execute_sql text (fun () ->
         check_ok ~sql db (Sqlite3.exec db.db) sql))
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
           match Sqlite3.step stmt with
               Sqlite3.Rc.ROW ->
                 begin try_lwt
                   check_num_cols "fold" stmt expr >>
                   f acc (snd expr.get_data (Sqlite3.row_data stmt))
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
           match Sqlite3.step stmt with
               Sqlite3.Rc.ROW ->
                 begin try_lwt
                   check_num_cols "iter" stmt expr >>
                   f (snd expr.get_data (Sqlite3.row_data stmt))
                 end >>= loop
             | Sqlite3.Rc.DONE -> return ()
             | rc -> raise_error db ~sql ~params rc
         in ensure_reset_stmt stmt loop ())
      db
      expr.statement
end
