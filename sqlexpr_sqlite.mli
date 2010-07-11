type db

exception Error of exn
exception Sqlite_error of string * Sqlite3.Rc.t

val open_db : string -> db
val close_db : db -> unit

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

module Sqlexpr :
sig
  type ('a, 'b) statement

  type ('a, 'b, 'c) expression

  val make_statement :
    cacheable:bool -> string ->
    ('a, 'b) Directives.directive -> ('a, 'b) statement

  val make_expression :
    ('a, 'c) statement -> int -> (Sqlite3.Data.t array -> 'b) ->
    ('a, 'b, 'c) expression
end

open Sqlexpr

val execute : db -> ('a, unit) statement -> 'a
val insert : db -> ('a, int64) statement -> 'a

val select_f : db -> ('a -> 'b) -> ('c, 'a, 'b list) expression -> 'c
val select : db -> ('c, 'a, 'a list) expression -> 'c
val select_one : db -> ('c, 'a, 'a) expression -> 'c
val iter : db -> ('a -> unit) -> ('c, 'a, unit) expression -> 'c
val fold : db -> ('a -> 'b -> 'a) -> 'a -> ('c, 'b, 'a) expression -> 'c

val transaction : db -> (db -> 'a) -> 'a

module Monadic :
  functor
    (M : sig
           type 'a t
           val return : 'a -> 'a t
           val bind : 'a t -> ('a -> 'b t) -> 'b t
           val fail : exn -> 'a t
           val catch : (unit -> 'a t) -> (exn -> 'a t) -> 'a t
           val finalize : (unit -> 'a t) -> (unit -> 'a t) -> 'a t
         end) ->
sig
  val fold :
    db -> ('a -> 'b -> 'a M.t) -> 'a -> ('c, 'b, 'a M.t) expression -> 'c

  val iter : db -> ('a -> unit M.t) -> ('b, 'a, unit M.t) expression -> 'b

  val transaction : db -> (db -> unit M.t) -> unit M.t
end

