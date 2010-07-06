
open Printf
open ExtList

type db = Sqlite3.db

let open_db fname = Sqlite3.db_open fname

let close_db db =
  try
    ignore (Sqlite3.db_close db)
  with Sqlite3.Error _ -> ()

module Directives =
struct
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

  let int k st n = param (fun n -> Sqlite3.Data.INT (Int64.of_int n)) k st n

  let int64 k st n = param (fun n -> Sqlite3.Data.INT n) k st n

  let int32 k st n = param (fun n -> Sqlite3.Data.INT (Int64.of_int32 n)) k st n

  let text k st s = param (fun n -> Sqlite3.Data.TEXT s) k st s

  let blob k st s = param (fun n -> Sqlite3.Data.BLOB s) k st s

  let float k st f = param (fun n -> Sqlite3.Data.FLOAT f) k st f

  let bool k st b = param (fun b -> Sqlite3.Data.INT (if b then 1L else 0L)) k st b

  let any k st f x = blob k st (f x)
end

module Conversion =
struct
  open Sqlite3.Data

  let error s =
    failwith
      (sprintf "Sqlexpr_sqlite error: bad data (expected %s)" s)

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

  let maybe f = function NULL -> None | x -> Some (f x)

  let maybe_text = maybe text
  let maybe_blob = maybe blob
  let maybe_int = maybe int
  let maybe_int32 = maybe int32
  let maybe_int64 = maybe int64
  let maybe_float = maybe float
  let maybe_bool = maybe bool
end

open Directives

module Sqlexpr =
struct
  type ('a, 'b) statement = ('a, 'b) Directives.statement

  type ('a, 'b, 'c) expression = {
    statement : ('a, 'c) statement;
    get_data : int * (Sqlite3.Data.t array -> 'b);
  }

  let make_statement ~cacheable sql directive =
    {
      sql_statement = sql;
      cacheable = cacheable;
      prepared_statement = ref None;
      directive = directive;
    }

  let make_expression stmt n f = { statement = stmt; get_data = (n, f) }

  let prepare db f (params, nparams, sql, prep) =
    let stmt = match prep with
        None -> Sqlite3.prepare db sql
      | Some r -> match !r with
            Some stmt ->
              (* FIXME: check return code *)
              ignore (Sqlite3.reset stmt);
              stmt
          | None ->
              let stmt = Sqlite3.prepare db sql in
                r := Some stmt;
                stmt
    in
      List.iteri
        (fun n v -> ignore (Sqlite3.bind stmt (nparams - n) v))
        params;
      f stmt

  let do_select f db p =
    p.directive (prepare db f)
      ([], 0, p.sql_statement,
       if p.cacheable then Some p.prepared_statement else None)

  let execute db (p : ('a, unit) statement) =
    do_select (fun stmt -> ignore (Sqlite3.step stmt)) db p

  let insert db p =
    do_select
      (fun stmt -> ignore (Sqlite3.step stmt); Sqlite3.last_insert_rowid db)
      db p

  let check_num_cols s stmt expr =
    let expected = fst expr.get_data in
    let actual = Array.length (Sqlite3.row_data stmt) in
      if expected <> actual then
        failwith
          (sprintf "Sqlexpr_sqlite.%s: wrong number of columns \
                    (expected %d, got %d)" s expected actual)

  let select_f db f expr =
    do_select
      (fun stmt ->
         let rec loop l =
           match Sqlite3.step stmt with
               (* FIXME: error checking *)
               Sqlite3.Rc.ROW ->
                 check_num_cols "select" stmt expr;
                 loop (f (snd expr.get_data (Sqlite3.row_data stmt)) :: l)
             | _ -> List.rev l
         in loop [])
      db
      expr.statement

  let select db expr = select_f db (fun x -> x) expr

  let iter db f expr =
    do_select
      (fun stmt ->
         let rec loop () =
           match Sqlite3.step stmt with
               (* FIXME: error checking *)
               Sqlite3.Rc.ROW ->
                 check_num_cols "iter" stmt expr;
                 f (snd expr.get_data (Sqlite3.row_data stmt));
                 loop ()
             | _ -> ()
         in loop ())
      db
      expr.statement

  let fold db f init expr =
    do_select
      (fun stmt ->
         let rec loop acc =
           match Sqlite3.step stmt with
               (* FIXME: error checking *)
               Sqlite3.Rc.ROW ->
                 check_num_cols "fold" stmt expr;
                 loop (f init (snd expr.get_data (Sqlite3.row_data stmt)))
             | _ -> acc
         in loop init)
      db
      expr.statement
end

include Sqlexpr

let new_tx_id =
  let pid = Unix.getpid () in
  let n = ref 0 in
    fun () -> incr n; sprintf "__sqlexpr_sqlite_tx_%d_%d" pid !n

let transaction db f =
  let txid = new_tx_id () in
    execute db sql"SAVEPOINT %s" txid;
    try
      let x = f db in
        execute db sql"RELEASE %s" txid;
        x
    with e ->
      execute db sql"ROLLBACK TO %s" txid;
      raise e

module Monadic(M : sig
                 type 'a t
                 val return : 'a -> 'a t
                 val bind : 'a t -> ('a -> 'b t) -> 'b t
                 val fail : exn -> 'a t
                 val catch : (unit -> 'a t) -> (exn -> 'a t) -> 'a t
                 val finalize : (unit -> 'a t) -> (unit -> 'a t) -> 'a t
               end)
=
struct
  module Lwt = M
  open Lwt

  let (>>=) = bind

  let fold db f init expr =
    do_select
      (fun stmt ->
         try_lwt
           let rec loop acc =
             match Sqlite3.step stmt with
                 (* FIXME: error checking *)
                 Sqlite3.Rc.ROW ->
                   check_num_cols "fold" stmt expr;
                   f init (snd expr.get_data (Sqlite3.row_data stmt)) >>= loop
               | _ -> return acc
           in loop init)
      db
      expr.statement

  let iter db f expr =
    do_select
      (fun stmt ->
         try_lwt
           let rec loop () =
             match Sqlite3.step stmt with
                 (* FIXME: error checking *)
                 Sqlite3.Rc.ROW ->
                   check_num_cols "fold" stmt expr;
                   f (snd expr.get_data (Sqlite3.row_data stmt)) >>= loop
               | _ -> return ()
           in loop ())
      db
      expr.statement

  let transaction db f =
    let txid = new_tx_id () in
      execute db sql"SAVEPOINT %s" txid;
      try_lwt
        lwt x = f db in
          execute db sql"RELEASE %s" txid;
          return x
      finally
        execute db sql"ROLLBACK TO %s" txid;
        return ()

end


