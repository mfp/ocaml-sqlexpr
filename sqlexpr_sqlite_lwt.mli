(** {!Sqlexpr_sqlite.S} implementation for the Lwt monad that uses thread
  * pools to avoid blocking on sqlite3 API calls. *)

include Sqlexpr_sqlite.S with type 'a result = 'a Lwt.t
