(** {!Sqlexpr_sqlite.S} implementation for the Lwt monad that uses thread
  * pools to avoid blocking on sqlite3 API calls. *)

include Sqlexpr_sqlite.S with type 'a result = 'a Lwt.t

(** [set_max_threads n] sets the maximum number of threads to
  * [max n current_thread_count] and returns the new limit *)
val set_max_threads : int -> int
