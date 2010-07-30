
open Printf
open OUnit

module Sqlexpr = Sqlexpr_sqlite.Make(Sqlexpr_concurrency.Id)

let aeq_int = assert_equal ~printer:(sprintf "%d")
let aeq_str = assert_equal ~printer:(sprintf "%S")
let aeq_float = assert_equal ~printer:(sprintf "%f")
let aeq_int32 = assert_equal ~printer:(sprintf "%ld")
let aeq_int64 = assert_equal ~printer:(sprintf "%Ld")
let aeq_bool = assert_equal ~printer:string_of_bool

let aeq_list ~printer =
  assert_equal
    ~printer:(fun l -> "[ " ^ String.concat "; " (List.map printer l) ^ " ]")

module S =
struct
  include Sqlexpr
  include Sqlexpr_sqlite
end

let with_db f x =
  let db = S.open_db ":memory:" in
    Sqlexpr_concurrency.Id.finalize
      (fun () -> f db x)
      (fun () -> S.close_db db)

let test_execute () =
  with_db
    (fun db () ->
       S.execute db sql"CREATE TABLE foo(id INTEGER PRIMARY KEY)";
       S.execute db sqlc"CREATE TABLE bar(id INTEGER PRIMARY KEY)")
    ()

let insert_d db l =
   S.execute db sql"CREATE TABLE foo(id INTEGER PRIMARY KEY, v INTEGER)";
   List.iter (S.execute db sql"INSERT INTO foo(v) VALUES(%d)") l

let insert_l db l =
   S.execute db sql"CREATE TABLE foo(id INTEGER PRIMARY KEY, v INTEGER)";
   List.iter (S.execute db sql"INSERT INTO foo(v) VALUES(%l)") l

let insert_L db l =
   S.execute db sql"CREATE TABLE foo(id INTEGER PRIMARY KEY, v INTEGER)";
   List.iter (S.execute db sql"INSERT INTO foo(v) VALUES(%L)") l

let insert_f db l =
   S.execute db sql"CREATE TABLE foo(id INTEGER PRIMARY KEY, v FLOAT)";
   List.iter (S.execute db sql"INSERT INTO foo(v) VALUES(%f)") l

let insert_s db l =
   S.execute db sql"CREATE TABLE foo(id INTEGER PRIMARY KEY, v TEXT)";
   List.iter (S.execute db sql"INSERT INTO foo(v) VALUES(%s)") l

let insert_S db l =
   S.execute db sql"CREATE TABLE foo(id INTEGER PRIMARY KEY, v BLOB)";
   List.iter (S.execute db sql"INSERT INTO foo(v) VALUES(%S)") l

let insert_b db l =
   S.execute db sql"CREATE TABLE foo(id INTEGER PRIMARY KEY, v BOOLEAN)";
   List.iter (S.execute db sql"INSERT INTO foo(v) VALUES(%b)") l

let test_directive_d () = with_db insert_d [1]
let test_directive_l () = with_db insert_l [1l]
let test_directive_L () = with_db insert_L [1L]
let test_directive_f () = with_db insert_f [3.14]
let test_directive_s () = with_db insert_s ["foo"]
let test_directive_S () = with_db insert_S ["blob"]
let test_directive_b () = with_db insert_b [true]

let test_oexpr fmt insert expr l () =
  with_db
    (fun db () ->
       let n = ref 1 in
         insert db l;
         let l = List.map (fun x -> let i = !n in incr n; (i, x)) l in
         let l' = List.sort compare (S.select db expr) in
           aeq_list
             ~printer:(fun (id, x) -> sprintf ("(%d, " ^^ fmt ^^ ")") id x)
            l l')
    ()

let test_nullable_oexpr fmt insert expr l () =
  with_db
    (fun db () ->
       let n = ref 1 in
         insert db l;
         let l = List.map (fun x -> let i = !n in incr n; (i, Some x)) l in
         let l' = List.sort compare (S.select db expr) in
           aeq_list
             ~printer:(fun (id, x) -> match x with
                           None -> sprintf "(%d, None)" id
                         | Some x -> sprintf ("(%d, Some " ^^ fmt ^^ ")") id x)
            l l')
    ()

let test_oexpr_directives =
  with_db
    (fun db () ->
       aeq_list ~printer:(sprintf "%d") [42] (S.select db sql"SELECT @d{%d}" 42);
       aeq_list ~printer:(sprintf "%f") [42.] (S.select db sql"SELECT @f{%d}" 42);
       aeq_list ~printer:(sprintf "%s") ["42"] (S.select db sql"SELECT @s{%d}" 42))

let test_directives =
  [
    "%d" >:: test_directive_d;
    "%l" >:: test_directive_l;
    "%L" >:: test_directive_L;
    "%f" >:: test_directive_f;
    "%s" >:: test_directive_s;
    "%S" >:: test_directive_S;
    "%b" >:: test_directive_b;
  ]

let test_outputs =
  let t = test_oexpr in
  let tn = test_nullable_oexpr in
    [
      "%d" >:: t "%d" insert_d sql"SELECT @d{id}, @d{v} FROM foo" [1;-1;3;4];
      "%l" >:: t "%ld" insert_l sql"SELECT @d{id}, @l{v} FROM foo" [1l;-1l;3l;4l];
      "%L" >:: t "%Ld" insert_L sql"SELECT @d{id}, @L{v} FROM foo" [1L;-1L;3L;4L];
      "%f" >:: t "%f" insert_f sql"SELECT @d{id}, @f{v} FROM foo" [1.;-1.; 10.; 1e2];
      "%s" >:: t "%s" insert_s sql"SELECT @d{id}, @s{v} FROM foo" ["foo"; "bar"; "baz"];
      "%S" >:: t "%S" insert_s sql"SELECT @d{id}, @S{v} FROM foo" ["foo"; "bar"; "baz"];
      "%b" >:: t "%b" insert_b sql"SELECT @d{id}, @b{v} FROM foo" [true; false];

      (* nullable *)
      "%d" >:: tn "%d" insert_d sql"SELECT @d{id}, @d?{v} FROM foo" [1;-1;3;4];
      "%l" >:: tn "%ld" insert_l sql"SELECT @d{id}, @l?{v} FROM foo" [1l;-1l;3l;4l];
      "%L" >:: tn "%Ld" insert_L sql"SELECT @d{id}, @L?{v} FROM foo" [1L;-1L;3L;4L];
      "%f" >:: tn "%f" insert_f sql"SELECT @d{id}, @f?{v} FROM foo" [1.;-1.; 10.; 1e2];
      "%s" >:: tn "%s" insert_s sql"SELECT @d{id}, @s?{v} FROM foo" ["foo"; "bar"; "baz"];
      "%S" >:: tn "%S" insert_s sql"SELECT @d{id}, @S?{v} FROM foo" ["foo"; "bar"; "baz"];
      "%b" >:: tn "%b" insert_b sql"SELECT @d{id}, @b?{v} FROM foo" [true; false];
    ]


let all_tests =
  [
    "Directives" >::: test_directives;
    "Outputs" >::: test_outputs;
    "Directives in output exprs" >:: test_oexpr_directives;
  ]

let _ =
  run_test_tt_main ("All" >::: all_tests)
