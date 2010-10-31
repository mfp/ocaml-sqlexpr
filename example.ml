
module Sqlexpr = Sqlexpr_sqlite.Make(Sqlexpr_concurrency.Id)
module S = Sqlexpr

let init_db db =
  S.execute db
    sqlinit"CREATE TABLE IF NOT EXISTS users(
              id INTEGER PRIMARY KEY,
              login TEXT UNIQUE,
              password TEXT NON NULL,
              name TEXT,
              email TEXT
            );"

let fold_users db f acc =
  S.fold db (fun acc x -> f x) acc
    sql"SELECT @s{login}, @s{password}, @s?{email} FROM users"

let insert_user db ~login ~password ?name ?email () =
  S.insert db
    sqlc"INSERT INTO users(login, password, name, email)
         VALUES(%s, %s, %s?, %s?)"
    login password name email

let auto_init_db, check_db, auto_check_db = sql_check"sqlite"


