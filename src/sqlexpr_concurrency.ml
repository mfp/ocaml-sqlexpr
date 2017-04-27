
module type THREAD_LOCAL_STATE =
sig
  type 'a t
  type 'a key
  val new_key : unit -> 'a key
  val get : 'a key -> 'a option
  val with_value : 'a key -> 'a option -> (unit -> 'b t) -> 'b t
end

module type THREAD =
sig
  type 'a t
  val return : 'a -> 'a t
  val bind : 'a t -> ('a -> 'b t) -> 'b t
  val try_bind: (unit -> 'a t) -> ('a -> 'b t) -> (exn -> 'b t) -> 'b t
  val fail : exn -> 'a t
  val catch : (unit -> 'a t) -> (exn -> 'a t) -> 'a t
  val finalize : (unit -> 'a t) -> (unit -> unit t) -> 'a t
  val sleep : float -> unit t
  val auto_yield : float -> (unit -> unit t)

  val backtrace_bind: (exn -> exn) -> 'a t -> ('a -> 'b t) -> 'b t
  val backtrace_catch: (exn -> exn) -> (unit -> 'a t) -> (exn -> 'a t) -> 'a t
  val backtrace_finalize: (exn -> exn) -> (unit -> 'a t) -> (unit -> unit t) -> 'a t
  val backtrace_try_bind: (exn -> exn) -> (unit -> 'a t) -> ('a -> 'b t) -> (exn -> 'b t) -> 'b t

  type mutex

  val create_recursive_mutex : unit -> mutex
  val with_lock : mutex -> (unit -> 'a t) -> 'a t

  val register_finaliser : ('a -> unit t) -> 'a -> unit

  include THREAD_LOCAL_STATE with type 'a t := 'a t
end

module Id =
struct
  type 'a t = 'a
  let return x = x
  let bind x f = f x
  let fail = raise

  let catch f g = try f () with e -> g e

  let finalize f g =
    match try `Ok (f ()) with e -> `Exn e with
        `Ok x -> g (); x
      | `Exn e -> g (); raise e

  let sleep dt = let _, _, _ = Unix.select [] [] [] dt in ()

  let auto_yield _ = (fun () -> ())

  type mutex = unit
  let create_recursive_mutex () = ()
  let with_lock () f = f ()

  type 'a key = 'a Lwt.key

  let new_key    = Lwt.new_key
  let get        = Lwt.get
  let with_value = Lwt.with_value

  let try_bind f f' catch =
    try f' (f ())
    with e -> catch e

  let backtrace_bind g x f =
    try f x
    with e -> raise (g e)

  let backtrace_catch g f catch =
    try f ()
    with e -> catch (g e)

  let backtrace_finalize g f finally =
    try
      let x = f() in
      finally();
      x
    with e -> raise (g e)

  let backtrace_try_bind g f f' catch =
    try
      let x = f() in
      f' x
    with e ->
      catch (g e)

  let register_finaliser f x =
    (* FIXME: should run finalisers sequentially in separate thread *)
    Gc.finalise f x
end


module Lwt =
struct
  include Lwt
  let auto_yield = Lwt_unix.auto_yield
  let sleep = Lwt_unix.sleep

  type mutex = { id : int; m : Lwt_mutex.t }

  let new_id = let n = ref 0 in (fun () -> incr n; !n)

  module LOCKS = Set.Make(struct
                            type t = int
                            let compare (x : int) y =
                              if x < y then -1 else if x > y then 1 else 0
                          end)
  let locks = Lwt.new_key ()

  let create_recursive_mutex () = { id = new_id (); m = Lwt_mutex.create () }

  let with_lock m f =
    match Lwt.get locks with
        None ->
          Lwt_mutex.with_lock m.m
            (fun () -> Lwt.with_value locks (Some (LOCKS.singleton m.id)) f)
      | Some s when LOCKS.mem m.id s -> f ()
      | Some s ->
          Lwt_mutex.with_lock m.m
            (fun () -> Lwt.with_value locks (Some (LOCKS.add m.id s)) f)

  let register_finaliser = Lwt_gc.finalise

  let fail e = fail (try raise e with exn -> exn)
end

