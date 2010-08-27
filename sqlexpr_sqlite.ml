
open Printf
open ExtList

exception Error of exn
exception Sqlite_error of string * Sqlite3.Rc.t

type db = Sqlite3.db

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

let open_db fname = Sqlite3.db_open fname

let close_db db =
  try
    ignore (Sqlite3.db_close db)
  with Sqlite3.Error _ -> ()

let sqlite_db db = db

module type THREAD = Sqlexpr_concurrency.THREAD

module Error(M : THREAD) =
struct
  let raise_error errcode =
    M.fail (Error (Sqlite_error (Sqlite3.Rc.to_string errcode, errcode)))

  let raise_exn exn = M.fail (Error exn)

  let failwithfmt fmt = Printf.ksprintf (fun s -> M.fail (Error (Failure s))) fmt
end

module Directives =
struct
  module D = Sqlite3.Data

  type st = (Sqlite3.Data.t list * int * string * Sqlite3.stmt option ref option)

  and ('a, 'b) statement =
      {
        sql_statement : string;
        cacheable : bool;
        prepared_statement : Sqlite3.stmt option ref;
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

  let profile_ch =
    try
      Some (open_out_gen [Open_append; Open_creat; Open_binary] 0o644
              (Unix.getenv "OCAML_SQLEXPR_PROFILE"))
    with Not_found -> None

  let prettify_sql_stmt sql =
    let sql = String.copy sql in
      for i = 0 to String.length sql - 1 do
        match sql.[i] with
            '\r' | '\n' | '\t' -> sql.[i] <- ' '
              | _ -> ()
      done;
      sql

  let string_of_param = function
      Sqlite3.Data.NONE -> "NONE"
    | Sqlite3.Data.NULL -> "NULL"
    | Sqlite3.Data.INT n -> Int64.to_string n
    | Sqlite3.Data.FLOAT f -> string_of_float f
    | Sqlite3.Data.TEXT s | Sqlite3.Data.BLOB s -> sprintf "%S" s

  let string_of_params l = String.concat ", " (List.map string_of_param l)

  (* accept a reversed list of params *)
  let profile sql ?(params = []) f =
    match profile_ch with
        None -> f ()
      | Some ch ->
          let sql = prettify_sql_stmt sql in
          let t0 = Unix.gettimeofday () in
          begin match params with
              [] -> Printf.fprintf ch "XXX\t%s\n%!" sql
            | l -> Printf.fprintf ch "XXX\t%s with params %s\n%!"
                     sql (string_of_params (List.rev l))
          end;
          let y = f () in
          let dt = Unix.gettimeofday () -. t0 in
            Printf.fprintf ch "%8.6f\t%s\n%!" dt sql;
            y

  let make_statement ~cacheable sql directive =
    {
      sql_statement = sql;
      cacheable = cacheable;
      prepared_statement = ref None;
      directive = directive;
    }

  let make_expression stmt n f = { statement = stmt; get_data = (n, f) }

  let sleep dt = let _, _, _ = Unix.select [] [] [] dt in ()

  let rec check_ok ?stmt f x = match f x with
      Sqlite3.Rc.OK | Sqlite3.Rc.DONE -> return ()
    | Sqlite3.Rc.BUSY ->
        sleep 0.002; (* FIXME: use the monad's sleep *)
        check_ok f x
    | code ->
        Option.may (fun stmt -> ignore (Sqlite3.reset stmt)) stmt;
        raise_error code

  let prepare db f (params, nparams, sql, prep) =
    lwt stmt =
      try_lwt
        match prep with
            None -> return (Sqlite3.prepare db sql)
          | Some r -> match !r with
                Some stmt -> check_ok ~stmt Sqlite3.reset stmt >> return stmt
              | None ->
                  let stmt = Sqlite3.prepare db sql in
                    r := Some stmt;
                    return stmt
      with e ->
        failwithfmt "Error with SQL statement %S:\n%s" sql (Printexc.to_string e) in
    let rec iteri ?(i = 0) f = function
        [] -> return ()
      | hd :: tl -> f i hd >> iteri ~i:(i + 1) f tl
    in
      (* the list of params is reversed *)
      iteri (fun n v -> check_ok ~stmt (Sqlite3.bind stmt (nparams - n)) v) params >>
      profile sql ~params (fun () -> f stmt)

  let do_select f db p =
    p.directive (prepare db f)
      ([], 0, p.sql_statement,
       if p.cacheable then Some p.prepared_statement else None)

  let execute db (p : ('a, unit M.t) statement) =
    do_select (fun stmt -> check_ok ~stmt Sqlite3.step stmt) db p

  let insert db p =
    do_select
      (fun stmt -> check_ok ~stmt Sqlite3.step stmt >> return (Sqlite3.last_insert_rowid db))
      db p

  let check_num_cols s stmt expr =
    let expected = fst expr.get_data in
    let actual = Array.length (Sqlite3.row_data stmt) in
      if expected = actual then return ()
      else
        failwithfmt
          "Sqlexpr_sqlite.%s: wrong number of columns \
           (expected %d, got %d)" s expected actual

  let select_f db f expr =
    do_select
      (fun stmt ->
         let rec loop l =
           match Sqlite3.step stmt with
               Sqlite3.Rc.ROW ->
                 check_num_cols "select" stmt expr >>
                 lwt x = f (snd expr.get_data (Sqlite3.row_data stmt)) in
                   loop (x :: l)
             | Sqlite3.Rc.DONE -> return (List.rev l)
             | rc -> raise_error rc
         in loop [])
      db
      expr.statement

  let select db expr = select_f db (fun x -> x) expr

  let select_one db expr =
    do_select
      (fun stmt ->
         match Sqlite3.step stmt with
             Sqlite3.Rc.ROW ->
               let r = snd expr.get_data (Sqlite3.row_data stmt) in
                 ignore (Sqlite3.reset stmt);
                 r
           | Sqlite3.Rc.DONE -> M.fail Not_found
           | rc -> raise_error rc)
      db
      expr.statement

  let iter db f expr =
    do_select
      (fun stmt ->
         let rec loop () =
           match Sqlite3.step stmt with
               Sqlite3.Rc.ROW ->
                 check_num_cols "iter" stmt expr >>
                 f (snd expr.get_data (Sqlite3.row_data stmt)) >>
                 loop ()
             | Sqlite3.Rc.DONE -> return ()
             | rc -> raise_error rc
         in loop ())
      db
      expr.statement

  let fold db f init expr =
    do_select
      (fun stmt ->
         let rec loop acc =
           match Sqlite3.step stmt with
               Sqlite3.Rc.ROW ->
                 check_num_cols "fold" stmt expr >>
                 loop (f init (snd expr.get_data (Sqlite3.row_data stmt)))
             | Sqlite3.Rc.DONE -> acc
             | rc -> raise_error rc
         in loop init)
      db
      expr.statement

  let new_tx_id =
    let pid = Unix.getpid () in
    let n = ref 0 in
      fun () -> incr n; sprintf "__sqlexpr_sqlite_tx_%d_%d" pid !n

  let unsafe_execute db fmt =
    ksprintf (fun sql -> profile sql (fun () -> check_ok (Sqlite3.exec db) sql)) fmt

  let transaction db f =
    let txid = new_tx_id () in
      unsafe_execute db "SAVEPOINT %s" txid >>
      try_lwt
        lwt x = f db in
          unsafe_execute db "RELEASE %s" txid >>
          return x
      with e ->
        unsafe_execute db "ROLLBACK TO %s" txid >>
        unsafe_execute db "RELEASE %s" txid >>
        fail e

  let (>>=) = bind

  let fold db f init expr =
    do_select
      (fun stmt ->
         let rec loop acc =
           match Sqlite3.step stmt with
               Sqlite3.Rc.ROW ->
                 begin try_lwt
                   check_num_cols "fold" stmt expr >>
                   f init (snd expr.get_data (Sqlite3.row_data stmt))
                 end >>= loop
             | Sqlite3.Rc.DONE -> return acc
             | rc -> try_lwt raise_error rc
         in loop init)
      db
      expr.statement

  let iter db f expr =
    do_select
      (fun stmt ->
         let rec loop () =
           match Sqlite3.step stmt with
               Sqlite3.Rc.ROW ->
                 begin try_lwt
                   check_num_cols "fold" stmt expr >>
                   f (snd expr.get_data (Sqlite3.row_data stmt))
                 end >>= loop
             | Sqlite3.Rc.DONE -> return ()
             | rc -> try_lwt raise_error rc
         in loop ())
      db
      expr.statement
end
