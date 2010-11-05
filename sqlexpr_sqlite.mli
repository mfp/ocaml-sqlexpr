(** Sqlexpr access to SQLite databases. *)

(** Module with the types defined in Sqlexpr_sqlite, provided for convenience *)
module Types : sig
  (** Database handle. *)
  type db

  (** Type used internally. *)
  type st
end

type db = Types.db
type st = Types.st

(** All the exceptions raised by the code in {Sqlexpr_sqlite} are wrapped in
    Error except when indicated otherwise. *)
exception Error of exn

(** Errors reported by SQLite are converted into [Sqlite_error _] exceptions,
    so they can be matched with
    [try ... with Sqlexpr.Error (Sqlexpr.sqlite_error _)] *)
exception Sqlite_error of string * Sqlite3.Rc.t

(** Open the DB whose filename is given. [":memory:"] refers to an in-mem DB. *)
val open_db : string -> db

(** Close the DB and finalize all the associated prepared statements. *)
val close_db : db -> unit

(** Return the [Sqlite3.db] handle from a [db]. *)
val sqlite_db : db -> Sqlite3.db


module Make(M : Sqlexpr_concurrency.THREAD) :
sig
  (** Type of SQL statements (no output parameters). *)
  type ('a, 'b) statement =
      {
        sql_statement : string;
        stmt_id : string option;
        directive : (st -> 'b) -> st -> 'a;
      }

  (** Type of SQL expressions (output parameters). *)
  type ('a, 'b, 'c) expression =
      {
        statement : ('a, 'c) statement;
        get_data : int * (Sqlite3.Data.t array -> 'b);
      }

  (** Database type, provided for convenience. *)
  type db = Types.db

  (** Exception identical to the toplevel [Error], provided for convenience.
      Note that [Sqlexpr_sqlite.Error _] matches this exception. *)
  exception Error of exn

  (** Exception identical to the toplevel [Sqlite_error], provided for
      convenience.  Note that [Sqlexpr_sqlite.Sqlite_error _] matches this
      exception. *)
  exception Sqlite_error of string * Sqlite3.Rc.t
  (** Same as the top-level one, provided for convenience. *)
  val open_db : string -> db

  (** Same as the top-level one, provided for convenience. *)
  val close_db : db -> unit

  (** Same as the top-level one, provided for convenience. *)
  val sqlite_db : db -> Sqlite3.db

  (** Execute a SQL statement. *)
  val execute : db -> ('a, unit M.t) statement -> 'a

  (** Execute an INSERT SQL statement and return the last inserted row id.
      Example:
      [insert db sqlc"INSERT INTO users(name, pass) VALUES(%s, %s)" name pass]
      *)
  val insert : db -> ('a, int64 M.t) statement -> 'a

  (** "Select" a SELECT SQL expression and return a list of tuples; e.g.
       [select db sqlc"SELECT \@s\{name\}, \@s\{pass\} FROM users"]
       [select db sqlc"SELECT \@s\{pass\} FROM users WHERE id = %L" user_id]
      *)
  val select : db -> ('c, 'a, 'a list M.t) expression -> 'c

  (** [select_f db f expr ...] is similar to [select db expr ...] but maps the
      results using the provided [f] function. *)
  val select_f : db -> ('a -> 'b M.t) -> ('c, 'a, 'b list M.t) expression -> 'c

  (** [select_one db expr ...] takes the first result from
      [select db expr ...].  @raise Not_found if no row is found. *)
  val select_one : db -> ('c, 'a M.t, 'a M.t) expression -> 'c

  (** Run the provided function in a DB transaction. A rollback is performed
      if an exception is raised inside the transaction. *)
  val transaction : db -> (db -> 'a M.t) -> 'a M.t

  (** [fold db f a expr ...] is
      [f (... (f (f a r1) r2) ...) rN]
      where [rN] is the n-th row returned for the SELECT expression [expr]. *)
  val fold :
    db -> ('a -> 'b -> 'a M.t) -> 'a -> ('c, 'b, 'a M.t) expression -> 'c

  (** Iterate through the rows returned for the supplied expression. *)
  val iter : db -> ('a -> unit M.t) -> ('b, 'a, unit M.t) expression -> 'b

  (** Module used by the code generated for SQL literals. *)
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

  (** Module used by the code generated for SQL literals. *)
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

