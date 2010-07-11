type db

exception Error of exn
exception Sqlite_error of string * Sqlite3.Rc.t

val open_db : string -> db
val close_db : db -> unit

module Make(M : Sqlexpr_concurrency.THREAD) :
sig
  module Directives :
  sig
    type st
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
  end

  module Conversion :
  sig
    val text : Sqlite3.Data.t -> string M.t
    val blob : Sqlite3.Data.t -> string M.t
    val int : Sqlite3.Data.t -> int M.t
    val int32 : Sqlite3.Data.t -> int32 M.t
    val int64 : Sqlite3.Data.t -> int64 M.t
    val float : Sqlite3.Data.t -> float M.t
    val bool : Sqlite3.Data.t -> bool M.t
    val maybe : (Sqlite3.Data.t -> 'a M.t) -> Sqlite3.Data.t -> 'a option M.t
    val maybe_text : Sqlite3.Data.t -> string option M.t
    val maybe_blob : Sqlite3.Data.t -> string option M.t
    val maybe_int : Sqlite3.Data.t -> int option M.t
    val maybe_int32 : Sqlite3.Data.t -> int32 option M.t
    val maybe_int64 : Sqlite3.Data.t -> int64 option M.t
    val maybe_float : Sqlite3.Data.t -> float option M.t
    val maybe_bool : Sqlite3.Data.t -> bool option M.t
  end

  type ('a, 'b) statement

  type ('a, 'b, 'c) expression

  val make_statement :
    cacheable:bool -> string ->
    ('a, 'b) Directives.directive -> ('a, 'b) statement

  val make_expression :
    ('a, 'c) statement -> int -> (Sqlite3.Data.t array -> 'b) ->
    ('a, 'b, 'c) expression

  val execute : db -> ('a, unit M.t) statement -> 'a
  val insert : db -> ('a, int64 M.t) statement -> 'a

  val select_f : db -> ('a -> 'b M.t) -> ('c, 'a, 'b list M.t) expression -> 'c
  val select : db -> ('c, 'a M.t, 'a list M.t) expression -> 'c
  val select_one : db -> ('c, 'a M.t, 'a M.t) expression -> 'c

  val transaction : db -> (db -> unit M.t) -> unit M.t

  val fold :
    db -> ('a -> 'b -> 'a M.t) -> 'a -> ('c, 'b, 'a M.t) expression -> 'c
  val iter : db -> ('a -> unit M.t) -> ('b, 'a, unit M.t) expression -> 'b

  val transaction : db -> (db -> unit M.t) -> unit M.t
end

