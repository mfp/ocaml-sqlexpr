(** Sqlexpr access to SQLite databases. *)

(**/**)
module Types : sig
  (** Type used internally. *)
  type st = Sqlite3.Data.t list * int * string * string option

  type 'a row_batch =
    | Batch_complete of 'a list
    | Batch_partial of 'a list
    | Batch_error of 'a list * exn
end

type st = Types.st
(**/**)

(** All the exceptions raised by the code in {Sqlexpr_sqlite} are wrapped in
    Error except when indicated otherwise. *)
exception Error of string * exn

(** Errors reported by SQLite are converted into [Sqlite_error _] exceptions,
    so they can be matched with
    [try ... with Sqlexpr.Error (_, Sqlexpr.sqlite_error _)] *)
exception Sqlite_error of string * Sqlite3.Rc.t

(**  *)
module type S =
sig
  (** Concurrency monad value. *)
  type 'a result

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

  (** Database type *)
  type db

  (** Exception identical to the toplevel [Error], provided for convenience.
      Note that [Sqlexpr_sqlite.Error _] matches this exception. *)
  exception Error of string * exn

  (** Exception identical to the toplevel [Sqlite_error], provided for
      convenience.  Note that [Sqlexpr_sqlite.Sqlite_error _] matches this
      exception. *)
  exception Sqlite_error of string * Sqlite3.Rc.t


  (** Specify whether to retry operations by default when SQLite3 returns
    * BUSY. (As of 0.6.0, the modules supplied with sqlexpr use false as their
    * default value; will likely be changed to true in a subsequent release.) *)
  val set_retry_on_busy : bool -> unit

  (** Returns whether operations are retried by default when SQLite3 returns
    * BUSY. (As of 0.6.0, the modules supplied with sqlexpr use false as their
    * default value; will likely be changed to true in a subsequent release.) *)
  val get_retry_on_busy : unit -> bool

  (** Open the DB whose filename is given. [":memory:"] refers to an in-mem DB.
    * @param [init] function to be applied to [Sqlite3.db] handle(s) before
    * they are used (can be used to register functions or initialize schema in
    * in-mem tables. *)
  val open_db : ?init:(Sqlite3.db -> unit) -> string -> db

  (** Close the DB and finalize all the associated prepared statements. *)
  val close_db : db -> unit

  (** [borrow_worker db f] evaluates [f db'] where [db'] borrows a 'worker'
    * from [db] and [db'] is only valid inside [f]. All the operations on
    * [db'] will use the same worker. Use this e.g. if you have an in-mem
    * database and a number of operations that must go against the same
    * instance (since data is not shared across different [:memory:]
    * databases). [db'] will not spawn new workers and will be closed and
    * invalidated automatically. *)
  val borrow_worker : db -> (db -> 'a result) -> 'a result

  (** [steal_worker db f] is similar to [borrow_worker db f], but ensures
    * that [f] is given exclusive access to the worker while it is being
    * evaluated. *)
  val steal_worker : db -> (db -> 'a result) -> 'a result

  (** Execute a SQL statement. *)
  val execute : db -> ('a, unit result) statement -> 'a

  (** Execute an INSERT SQL statement and return the last inserted row id.
      Example:
      [insert db sqlc"INSERT INTO users(name, pass) VALUES(%s, %s)" name pass]
      *)
  val insert : db -> ('a, int64 result) statement -> 'a

  (** "Select" a SELECT SQL expression and return a list of tuples; e.g.
       [select db sqlc"SELECT \@s\{name\}, \@s\{pass\} FROM users"]
       [select db sqlc"SELECT \@s\{pass\} FROM users WHERE id = %L" user_id]
      *)
  val select : db -> ('c, 'a, 'a list result) expression -> 'c

  (** [select_f db f expr ...] is similar to [select db expr ...] but maps the
      results using the provided [f] function. *)
  val select_f : db -> ('a -> 'b result) -> ('c, 'a, 'b list result) expression -> 'c

  (** [select_one db expr ...] takes the first result from
      [select db expr ...].
      @raise Not_found if no row is found. *)
  val select_one : db -> ('c, 'a, 'a result) expression -> 'c

  (** [select_one_maybe db expr ...] takes the first result from
      [select db expr ...].
      @return None if no row is found. *)
  val select_one_maybe : db -> ('c, 'a, 'a option result) expression -> 'c

  (** [select_one_f db f expr ...] is returns the first result from
      [select_f db f expr ...].
      @raise Not_found if no row is found. *)
  val select_one_f : db -> ('a -> 'b result) -> ('c, 'a, 'b result) expression -> 'c

  (** [select_one_f_maybe db expr ...] takes the first result from
      [select_f db f expr ...].
      @return None if no row is found. *)
  val select_one_f_maybe : db -> ('a -> 'b result) ->
    ('c, 'a, 'b option result) expression -> 'c

  (** Run the provided function in a DB transaction. A rollback is performed
      if an exception is raised inside the transaction.

      If the BEGIN or COMMIT SQL statements from the outermost transaction fail
      with [SQLITE_BUSY], they will be retried until they can be executed.
      A [SQLITE_BUSY] (or any other) error code in any other operation inside
      a transaction will result in an [Error (_, Sqlite_error (code, _))]
      exception being thrown, and a rollback performed.

      One consequence of this is that concurrency control is very simple if
      you use [`EXCLUSIVE] transactions: the code can be written
      straightforwardly as [S.transaction db (fun db -> ...)], and their
      execution will be serialized (across both threads and processes).
      Note that, for [`IMMEDIATE] and [`DEFERRED] transactions, you will
      have to retry manually if an
      [Error (_, Sqlite_error (Sqlite3.Rc.Busy, _))] is raised.

      All SQL operations performed within a transaction will use the same
      worker.  This worker is used exclusively by only one thread per
      instantiated module (see {!steal_worker}).
      That is, given
        {[
           module S1 = Sqlexpr_sqlite.Make(Sqlexpr_concurrency.Id)
           module S2 = Sqlexpr_sqlite.Make(Sqlexpr_concurrency.Lwt)
           let db = S1.open_db somefile
        ]}
        there is no exclusion between functions from [S1] and those from [S2].

      @param kind transaction kind, only meaningful for outermost transaction
             (default [`DEFERRED])
      *)
  val transaction :
    db -> ?kind:[`DEFERRED | `IMMEDIATE | `EXCLUSIVE] ->
    (db -> 'a result) -> 'a result

  (** [fold db f a expr ...] is
      [f (... (f (f a r1) r2) ...) rN]
      where [rN] is the n-th row returned for the SELECT expression [expr]. *)
  val fold :
    db -> ('a -> 'b -> 'a result) -> 'a -> ('c, 'b, 'a result) expression -> 'c

  (** Same as {!fold}, but faster for some pool implementations because it
    * reads several rows at a time. *)
  val fold_batch :
    db -> ('a -> 'b -> 'a result) -> 'a -> ('c, 'b, 'a result) expression -> 'c

  (** Iterate through the rows returned for the supplied expression. *)
  val iter : db -> ('a -> unit result) -> ('b, 'a, unit result) expression -> 'b

  (** Iterate through the rows returned for the supplied expression.
    * For some worker pool implementations, this operation is faster then
    * {!iter} because several rows are read at a time (e.g., without detaching
    * each "next" operation). *)
  val iter_batch : db -> ('a -> unit result) -> ('b, 'a, unit result) expression -> 'b

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

(** [db] type shared by single-worker ("identity pool") {!S} implementations. *)
type single_worker_db

module Make : functor (M : Sqlexpr_concurrency.THREAD with type 'a key = 'a Lwt.key) ->
sig
  include S with type 'a result = 'a M.t and type db = single_worker_db

  val make : Sqlite3.db -> db

  (** Return the [Sqlite3.db] handle from a [db]. *)
  val sqlite_db : db -> Sqlite3.db
end

module type POOL =
sig
  type 'a result

  module TLS : Sqlexpr_concurrency.THREAD_LOCAL_STATE with type 'a t := 'a result

  type db
  type stmt

  val set_retry_on_busy : bool -> unit
  val get_retry_on_busy : unit -> bool

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
  val unsafe_execute : db -> ?retry_on_busy:bool -> string -> unit result
  val borrow_worker : db -> (db -> 'a result) -> 'a result
  val steal_worker : db -> (db -> 'a result) -> 'a result

  val transaction_key : db -> unit TLS.key

  val read_rows :
    (fname:string -> stmt -> sql:string -> Sqlite3.Data.t list ->
     cols:int -> (Sqlite3.Data.t array -> 'b) -> 'b Types.row_batch result) option
end

module Make_gen :
  functor (M : Sqlexpr_concurrency.THREAD) ->
    functor(P : POOL with type 'a result = 'a M.t) ->
      S with type 'a result = 'a M.t

(**/**)
val prettify_sql_stmt : string -> string
val string_of_param : Sqlite3.Data.t -> string
val string_of_params : Sqlite3.Data.t list -> string

module Stmt :
sig
  type t
  val prepare : Sqlite3.db -> string -> t
  val db_handle : t -> Sqlite3.db
  val finalize : t -> unit
  val reset : t -> Sqlite3.Rc.t
  val step : t -> Sqlite3.Rc.t
  val bind : t -> int -> Sqlite3.Data.t -> Sqlite3.Rc.t
  val row_data : t -> Sqlite3.Data.t array
end

module Stmt_cache :
sig
  type t
  val create : unit -> t
  val flush_stmts : t -> unit
  val find_remove_stmt : t -> string -> Stmt.t option
  val add_stmt : t -> string -> Stmt.t -> unit
end

module Profile : functor (M : Sqlexpr_concurrency.THREAD) ->
sig
  val profile_execute_sql :
    string -> ?full_sql:string -> ?params:Sqlite3.Data.t list ->
    (unit -> 'b M.t) -> 'b M.t
  val profile_prepare_stmt : string -> (unit -> 'a M.t) -> 'a M.t
end

(**/**)
