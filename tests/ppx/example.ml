open Lwt.Infix
module Sqlexpr = Sqlexpr_sqlite_lwt

let _ = [
  [%sqlinit "CREATE TABLE IF NOT EXISTS users (
              login TEXT PRIMARY KEY,
              password TEXT NON NULL)"]
]

let select_users_stmt =
  [%sqlc "SELECT @s{login}, @s{password} FROM users"]

let insert_user_stmt =
  [%sqlc "INSERT INTO users (login, password) VALUES (%s, %s)"]

let auto_init_db, check_db, auto_check_db = [%sqlcheck "sqlite"]

let gen_random_string =
  let buf = Cstruct.create 8 in
  fun () ->
    let i = Random.int64 Int64.max_int in
    Cstruct.BE.set_uint64 buf 0 i ;
    let `Hex hexstr = Hex.of_cstruct buf in
    hexstr

let open_db fn =
  Sqlexpr.open_db fn ~init:begin fun db ->
    let inited = auto_init_db db Format.err_formatter in
    let checked = check_db db Format.err_formatter in
    if not (inited && checked) then
      failwith "consistency checks failed for db `users`"
  end

let insert_users fn n =
  let db = open_db fn in
  let rec inner n =
    if n < 0 then Lwt.return_unit
    else begin
      let login = gen_random_string () in
      let passwd = gen_random_string () in
      Sqlexpr.execute db insert_user_stmt login passwd >>= fun () ->
      inner (pred n)
    end
  in inner n

let fold_users db =
  Format.eprintf "FOLD@." ;
  Sqlexpr.fold db begin fun () (user, passwd) ->
    Format.eprintf "%s %s@." user passwd ;
    Lwt.return_unit
  end () select_users_stmt

let fold_forever fn =
  Format.eprintf "FOLDFOREVRE %s@." fn ;
  let db = open_db fn in
  let rec inner () =
    fold_users db >>= fun () ->
    inner () in
  inner ()

let main () =
  Sqlexpr.set_retry_on_busy true ;
  if Array.length Sys.argv < 3 then begin
    Format.eprintf "Usage: %s <cmd> <args> (check source code)@." Sys.argv.(0) ;
    exit 1
  end ;
  match Sys.argv.(1) with
  | "fold" ->
    let fn = Sys.argv.(2) in
    fold_forever fn
  | "insert" ->
    let fn = Sys.argv.(2) in
    let n = int_of_string Sys.argv.(3) in
    Format.eprintf "Opening %s and add %d rows@." fn n ;
    insert_users fn n
  | _ ->
    invalid_arg "Usage: %s <cmd> <args> (check source code)"

let () =
  Lwt_main.run (main ())
