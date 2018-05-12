
open Printf
open Lwt

module SYNC =
struct
  module Sqlexpr = Sqlexpr_sqlite.Make(Sqlexpr_concurrency.Id)
  module S = Sqlexpr
end

let init db =
  let open SYNC in
  S.execute (S.make db) sql"CREATE TABLE foo(id INTEGER PRIMARY KEY, v TEXT)"

module BM(Sqlexpr : Sqlexpr_sqlite.S with type 'a result = 'a Lwt.t) =
struct
  module S = Sqlexpr

  let run label =
    let db    = S.open_db ~init ":memory:" in
    let n     = ref 0 in
    let rows  = 10_000 in
    let iters = 500 in
      for_lwt i = 1 to rows do
        S.execute db sqlc"INSERT INTO foo(v) VALUES(%s)" (string_of_int i)
      done >>= fun () ->
      let () = Gc.major () in
      let t0 = Unix.gettimeofday () in
        for_lwt i = 1 to iters do
          S.iter db (fun s -> n := !n + String.length s; return_unit)
              sqlc"SELECT @s{v} FROM foo"
        done >>= fun () ->
        let dt = Unix.gettimeofday () -. t0 in
          Lwt_io.printf "%s needed %5.2f (%.0f/s)\n" label dt
            (float (rows * iters) /. dt)
end

module DETACHED = BM(Sqlexpr_sqlite_lwt)
module NORMAL   = BM(Sqlexpr_sqlite.Make(Sqlexpr_concurrency.Lwt))

let () = Lwt_main.run (NORMAL.run "normal" >>= fun () -> DETACHED.run "detached")
