**ocaml-sqlexpr** is a simple library and syntax extension for type-safe,
convenient execution of SQL statements, currently compatible with Sqlite3.

The latest version can be found at https://github.com/mfp/ocaml-sqlexpr

**ocaml-sqlexpr** features:
* automated prepared statement caching, parameter binding, data extraction, error
  checking (including automatic statement reset to avoid BUSY/LOCKED errors in
  subsequent queries), statement finalization on database close, etc.
* higher order functions like *iter*, *fold*, *transaction*
* support for different concurrency models: everything is functorized over a
  THREAD monad, so you can for instance do concurrent treatmments with Lwt
* support for SQL statement syntax checks and some extra semantic checking (column
  names, etc.)

**ocaml-sqlexpr** is used as follows:

```ocaml
module Sqlexpr = Sqlexpr_sqlite.Make(Sqlexpr_concurrency.Id)
module S = Sqlexpr

let () =
  let db = S.open_db "foo.db" in
  S.iter db
    (fun (n, p) -> Printf.printf "User %S, password %S\n" n p)
    sqlc"SELECT @s{login}, @s{password} FROM users";
  List.iter
    (fun (n, p) -> S.execute db sqlc"INSERT INTO users VALUES(%s, %s)" n p)
    [
     "coder24", "badpass";
     "tokyo3", "12345"
    ]
```

See also the example file `example.ml`.


## Dependencies

* cppo
* csv
* lwt (>= 2.2.0)
* lwt.syntax
* lwt.unix
* ppx_tools
* re
* sqlite3
* threads
* unix

## Optional Dependencies

* camlp4
* estring

The optional dependencies allow building of the Camlp4 syntax extension.

## Camlp4 syntax extension

**ocaml-sqlexpr** includes a syntax extension to build type-safe SQL
statements and expressions:


- `sql"..."`   denotes a SQL statement or expression
- `sqlc"..."`  denotes a SQL statement or expression that is to be cached
- `sql_check"sqlite"` returns a tuple of functions to initialize, check the
                      validity of the SQL statements or expressions and
                      check against an auto-initialized temporary database.
- `sqlinit"..."` is equivalent to `sql"..."`, but the statement will be added
                 to the list of statements to be executed in the automatically
                 generated initialization function

`sql_check"sqlite"` is used as follows:

```ocaml
let auto_init_db, check_db, auto_check_db = sql_check"sqlite"
```

which creates 3 functions

```ocaml
val auto_init_db : Sqlite3.db -> Format.formatter -> bool
val check_db : Sqlite3.db -> Format.formatter -> bool
val auto_check_db : Format.formatter -> bool
```

each of them returns `false` on error, and writes the error messages to the
provided formatter.


## PPX syntax extension

In addition to the camlp4-based syntax extension, **ocaml-sqlexpr** includes a
syntax extension using extension points (ppx). The conversion from camlp4 to
ppx is as follows:

- `[%sql "..."]` corresponds to `sql"..."`
- `[%sqlc "..."]` corresponds to `sqlc"..."`
- `[%sqlcheck "sqlite"]` corresponds to `sql_check"sqlite"`
- `[%sqlinit "..."]` corresponds to `sqlinit"..."`


## SQL statement/expression syntax

Literals marked with `sql` or `sqlc` are similar to Printf's format strings and their precise
types depend on their contents. They accept input parameters (similarly to
Printf) and, in the case of SQL expressions, their execution will yield a
tuple whose type is determined by the output parameters.

Input parameters are denoted with `%X` where `X` is one of:

  Input parameter  | OCaml type
  -----------------|-----------
  %d               | int
  %l               | Int32.t
  %L               | Int64.t
  %s               | string
  %S               | string    (handled as BLOB by SQLite)
  %f               | float
  %b               | bool
  %a               | ('a -> string) (resulting string handled as BLOB by SQLite)

A literal `%` is denoted with `%%`.

A parameter is made nullable (turning the OCaml type into a `_ option`) by
appending a `?`, e.g. `%d?`.

Output parameters are denoted with `@X{SQL expression}` where `X` is one of:

  Output parameter | OCaml type
  ---------------- | ----------
  @d               | int
  @l               | Int32.t
  @L               | Int64.t
  @s               | string
  @S               | string    (handled as BLOB by SQLite)
  @f               | float
  @b               | bool

A literal `@` is denoted with `@@`
As in the case of input parameters, output parameters can be made nullable by
appending a `?`.

A `sql"..."` or `sqlc"..."` literal is of type `_ statement` if it has no output
parameters, and of type `_ expression` if it has at least one.


### Examples

```ocaml
sql"SELECT @s{name} FROM users"                   is an expression
sql"SELECT @s{name} FROM users WHERE id = %d"     is an expression
sql"SELECT @s{name}, @s{email} FROM users"        is an expression
sql"DELETE FROM users WHERE id = %d"              is a statement
```

Statements are executed with `execute` or `insert` (which returns the id of
the new row); expressions are “selected” with a function from the `select*`
family or a higher order function like `iter` or `fold`.


### Examples

```ocaml
module Sqlexpr = Sqlexpr_sqlite.Make(Sqlexpr_concurrency.Id)
module S = Sqlexpr

let insert_user_stmt =
    sqlc"INSERT INTO users(login, password, email) VALUES(%s, %s, %s?)"

let insert_user db ~login ?email ~password =
  S.execute db insert_user_stmt login password email

(* insert user and return ID; we use partial application here *)
let new_user_id db = S.insert db insert_user_stmt

let get_password db =
  S.select_one db sqlc"SELECT @s{password} FROM users WHERE login = %s"

let get_email db =
  S.select_one db sqlc"SELECT @s?{email} FROM users WHERE login = %s"

let iter_users db f =
  S.iter db f sqlc"SELECT @L{id}, @s{login}, @s{password}, @s?{email}
                     FROM users"
```

### Test and Sample Build Instructions

Example Camlp4 Code:
```
ocamlfind ocamlc -package sqlexpr,pa_sqlexpr -syntax camlp4o -linkpkg -thread -o sqlexpr_camlp4 tests/syntax/example.ml
```

Example PPX Code
```
ocamlfind ocamlc -package sqlexpr.ppx -linkpkg -thread -o sqlexpr_ppx tests/ppx/example.ml
```
or
```
jbuilder build tests/ppx/example.exe
```

Camlp4 based tests:
```
ocamlfind ocamlc -package sqlexpr,pa_sqlexpr,lwt.syntax,oUnit -syntax camlp4o -linkpkg -thread -o sqlexpr_camlp4_test tests/syntax/t_sqlexpr.ml
```

PPX based tests:
```
ocamlfind ocamlc -package sqlexpr.ppx,lwt.ppx,oUnit -ppxopt lwt.ppx,-no-debug -linkpkg -thread -o sqlexpr_ppx_test tests/ppx/t_sqlexpr.ml
```
or
```
jbuilder runtest ./tests/ppx
```
