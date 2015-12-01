
open Printf
open OUnit

let aeq_int = assert_equal ~printer:(sprintf "%d")
let aeq_str = assert_equal ~printer:(sprintf "%S")
let aeq_float = assert_equal ~printer:(sprintf "%f")
let aeq_int32 = assert_equal ~printer:(sprintf "%ld")
let aeq_int64 = assert_equal ~printer:(sprintf "%Ld")
let aeq_bool = assert_equal ~printer:string_of_bool

let aeq_list ~printer =
  assert_equal
    ~printer:(fun l -> "[ " ^ String.concat "; " (List.map printer l) ^ " ]")

module Test
  (Lwt : sig
     include Sqlexpr_concurrency.THREAD
     val iter : ('a -> unit t) -> 'a list -> unit t
     val run : 'a t -> 'a
   end)
  (Sqlexpr : sig
     include Sqlexpr_sqlite.S with type 'a result = 'a Lwt.t
   end) =
struct
  open Lwt
  module S = Sqlexpr

  let (>|=) f g = bind f (fun x -> return (g x))
  let (>>=) = Lwt.bind

  (* schema changes to :memory: db made by a Sqlexpr_sqlite_lwt worker are not
   * seen by the others, so allow to use a file by doing ~in_mem:false *)
  let with_db ?(in_mem = true) f x =
    let file =
      if in_mem then ":memory:" else Filename.temp_file "t_sqlexpr_sqlite_" "" in
    let db = S.open_db file in
    let%lwt () = try%lwt
        f db x
    with _ -> return () in
    S.close_db db;
    if not in_mem then Sys.remove file;
    return ()

  let test_execute () =
    with_db
      (fun db () ->
        S.execute db [%sql "CREATE TABLE foo(id INTEGER PRIMARY KEY)"] >>= fun () ->
        S.execute db [%sqlc "CREATE TABLE bar(id INTEGER PRIMARY KEY)"] >>= fun ()  ->
        return ())
      ()

  let insert_d db l =
    S.execute db [%sql "CREATE TABLE foo(id INTEGER PRIMARY KEY, v INTEGER)"] >>= fun () ->
    iter (S.execute db [%sql "INSERT INTO foo(v) VALUES(%d)"]) l

  let insert_l db l =
    S.execute db [%sql "CREATE TABLE foo(id INTEGER PRIMARY KEY, v INTEGER)"] >>= fun () ->
    iter (S.execute db [%sql "INSERT INTO foo(v) VALUES(%l)"]) l

  let insert_L db l =
    S.execute db [%sql "CREATE TABLE foo(id INTEGER PRIMARY KEY, v INTEGER)"] >>= fun () ->
    iter (S.execute db [%sql "INSERT INTO foo(v) VALUES(%L)"]) l

  let insert_f db l =
    S.execute db [%sql "CREATE TABLE foo(id INTEGER PRIMARY KEY, v FLOAT)"] >>= fun () ->
    iter (S.execute db [%sql "INSERT INTO foo(v) VALUES(%f)"]) l

  let insert_s db l =
    S.execute db [%sql "CREATE TABLE foo(id INTEGER PRIMARY KEY, v TEXT)"] >>= fun () ->
    iter (S.execute db [%sql "INSERT INTO foo(v) VALUES(%s)"]) l

  let insert_S db l =
    S.execute db [%sql "CREATE TABLE foo(id INTEGER PRIMARY KEY, v BLOB)"] >>= fun () ->
    iter (S.execute db [%sql "INSERT INTO foo(v) VALUES(%S)"]) l

  let insert_b db l =
    S.execute db [%sql "CREATE TABLE foo(id INTEGER PRIMARY KEY, v BOOLEAN)"] >>= fun () ->
    iter (S.execute db [%sql "INSERT INTO foo(v) VALUES(%b)"]) l

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
           insert db l >>= fun () ->
           let l = List.map (fun x -> let i = !n in incr n; (i, x)) l in
           let%lwt l' = S.select db expr in
           let l' = List.sort compare l' in
             aeq_list ~printer:(fun (id, x) -> sprintf ("(%d, " ^^ fmt ^^ ")") id x)
               l l';
             return ())
      ()

  let test_nullable_oexpr fmt insert expr l () =
    with_db
      (fun db () ->
         let n = ref 1 in
           insert db l >>= fun () ->
           let l = List.map (fun x -> let i = !n in incr n; (i, Some x)) l in
           let%lwt l' = S.select db expr in
           let l' = List.sort compare l' in
             aeq_list
               ~printer:(fun (id, x) -> match x with
                             None -> sprintf "(%d, None)" id
                           | Some x -> sprintf ("(%d, Some " ^^ fmt ^^ ")") id x)
               l l';
             return ())
      ()

  let test_oexpr_directives =
    with_db
      (fun db () ->
        S.select db [%sql "SELECT @d{%d}"] 42 >|= aeq_list ~printer:(sprintf "%d") [42] >>= fun () ->
        S.select db [%sql "SELECT @f{%d}"] 42 >|= aeq_list ~printer:(sprintf "%f") [42.] >>= fun () ->
        S.select db [%sql "SELECT @s{%d}"] 42 >|= aeq_list ~printer:(sprintf "%s") ["42"])

  let (>::) name f = name >:: (fun () -> run (f ()))

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
        "%d" >:: t "%d" insert_d [%sql "SELECT @d{id}, @d{v} FROM foo"] [1;-1;3;4];
        "%l" >:: t "%ld" insert_l [%sql "SELECT @d{id}, @l{v} FROM foo"] [1l;-1l;3l;4l];
        "%L" >:: t "%Ld" insert_L [%sql "SELECT @d{id}, @L{v} FROM foo"] [1L;-1L;3L;4L];
        "%f" >:: t "%f" insert_f [%sql "SELECT @d{id}, @f{v} FROM foo"] [1.;-1.; 10.; 1e2];
        "%s" >:: t "%s" insert_s [%sql "SELECT @d{id}, @s{v} FROM foo"] ["foo"; "bar"; "baz"];
        "%S" >:: t "%S" insert_s [%sql "SELECT @d{id}, @S{v} FROM foo"] ["foo"; "bar"; "baz"];
        "%b" >:: t "%b" insert_b [%sql "SELECT @d{id}, @b{v} FROM foo"] [true; false];

        (* nullable *)
        "%d" >:: tn "%d" insert_d [%sql "SELECT @d{id}, @d?{v} FROM foo"] [1;-1;3;4];
        "%l" >:: tn "%ld" insert_l [%sql "SELECT @d{id}, @l?{v} FROM foo"] [1l;-1l;3l;4l];
        "%L" >:: tn "%Ld" insert_L [%sql "SELECT @d{id}, @L?{v} FROM foo"] [1L;-1L;3L;4L];
        "%f" >:: tn "%f" insert_f [%sql "SELECT @d{id}, @f?{v} FROM foo"] [1.;-1.; 10.; 1e2];
        "%s" >:: tn "%s" insert_s [%sql "SELECT @d{id}, @s?{v} FROM foo"] ["foo"; "bar"; "baz"];
        "%S" >:: tn "%S" insert_s [%sql "SELECT @d{id}, @S?{v} FROM foo"] ["foo"; "bar"; "baz"];
        "%b" >:: tn "%b" insert_b [%sql "SELECT @d{id}, @b?{v} FROM foo"] [true; false];
      ]

  exception Cancel

  let test_transaction () =
    with_db begin fun db () ->
      let s_of_pair (id, data) = sprintf "(%d, %S)" id data in
      let get_rows db = S.select db [%sql "SELECT @d{id}, @s{data} FROM foo ORDER BY id"] in
      let get_one db = S.select_one db [%sql "SELECT @d{id}, @s{data} FROM foo ORDER BY id"] in
      let get_one' db = S.select_one db [%sqlc "SELECT @d{id}, @s{data} FROM foo ORDER BY id"] in
      let insert db = S.execute db [%sql "INSERT INTO foo(id, data) VALUES(%d, %s)"] in
      let aeq = aeq_list ~printer:s_of_pair in
      let aeq_one = assert_equal ~printer:s_of_pair in
        S.execute db [%sql "CREATE TABLE foo(id INTEGER NOT NULL, data TEXT NOT NULL)"] >>= fun () ->
        get_rows db >|= aeq ~msg:"Init" [] >>= fun () ->
        S.transaction db
          (fun db ->
             get_rows db >|= aeq [] >>= fun () ->
             insert db 1 "foo" >>= fun () ->
             get_rows db >|= aeq ~msg:"One insert in TX" [1, "foo"] >>= fun () ->
             get_one db >|= aeq_one ~msg:"select_one after 1 insert in TX" (1, "foo") >>= fun () ->
             get_one' db >|= aeq_one ~msg:"select_one (cached) after 1 insert in TX"
                               (1, "foo") >>= fun () ->
             try%lwt
               S.transaction db
                 (fun db ->
                    insert db 2 "bar" >>= fun () ->
                    get_rows db >|= aeq ~msg:"Insert in nested TX" [1, "foo"; 2, "bar";] >>= fun () ->
                    fail Cancel)
             with Cancel ->
               get_rows db >|= aeq ~msg:"After nested TX is canceled" [1, "foo"]) >>= fun () ->
        get_rows db >|= aeq [1, "foo"];
    end ()

  let test_retry_begin () =

    let count_rows db =
      S.select_one db [%sqlc "SELECT @d{COUNT(*)} FROM foo"] in

    let insert v db =
      (* SELECT acquires a SHARED lock if needed *)
      let%lwt _ = count_rows db in
        Lwt.sleep 0.010 >>= fun () ->
        (* RESERVED lock acquired if needed *)
        S.insert db [%sqlc "INSERT INTO foo VALUES(%d)"] v in

    let fname = Filename.temp_file "t_sqlexpr_sqlite_excl_retry" "" in
    let db1   = S.open_db fname in
    let db2   = S.open_db fname in

      S.execute db1 sqlc"CREATE TABLE foo(id INTEGER PRIMARY KEY)" >>= fun () ->
      (* these 2 TXs are serialized because they are EXCLUSIVE *)
      let%lwt _   = S.transaction ~kind:`EXCLUSIVE db1 (insert 1)
      and _   = S.transaction ~kind:`EXCLUSIVE db2 (insert 2) in
      let%lwt n   = count_rows db1 in
        aeq_int ~msg:"number of rows inserted" 2 n;
        return ()

  let test_fold_and_iter () =
    with_db begin fun db () ->
      S.execute db [%sql "CREATE TABLE foo(n INTEGER NOT NULL)"] >>= fun () ->
      let l = Array.to_list (Array.init 100 (fun n -> 1 + Random.int 100000)) in
        iter (S.execute db [%sqlc "INSERT INTO foo(n) VALUES(%d)"]) l >>= fun () ->
        let sum = List.fold_left (+) 0 l in
        let%lwt count, sum' =
          S.fold db
            (fun (count, sum) n -> return (count + 1, sum + n))
            (0, 0) sqlc"SELECT @d{n} FROM foo"
        in
          aeq_int ~msg:"fold: number of elements" (List.length l) count;
          aeq_int ~msg:"fold: sum of elements" sum sum';
          let count = ref 0 in
          let sum' = ref 0 in
          let%lwt () =
            S.iter db
              (fun n -> incr count; sum' := !sum' + n; return ())
              sqlc"SELECT @d{n} FROM foo"
          in
            aeq_int ~msg:"iter: number of elements" (List.length l) !count;
            aeq_int ~msg:"iter: sum of elements" sum !sum';
            return ()
    end ()

  let rec do_test_nested_iter_and_fold db () =
    nested_iter_and_fold_write db >>= fun () ->
    nested_iter_and_fold_read db

  and nested_iter_and_fold_write db =
    S.execute db [%sql "CREATE TABLE foo(n INTEGER NOT NULL)"] >>= fun () ->
    iter (S.execute db [%sqlc "INSERT INTO foo(n) VALUES(%d)"]) [1; 2; 3]

  and nested_iter_and_fold_read db =
    let q = Queue.create () in
    let expected =
      List.rev [ 1, 3; 1, 2; 1, 1; 2, 3; 2, 2; 2, 1; 3, 3; 3, 2; 3, 1; ] in
    let inner = [%sqlc "SELECT @d{n} FROM foo ORDER BY n DESC"] in
    let outer = [%sqlc "SELECT @d{n} FROM foo ORDER BY n ASC"] in
    let printer (a, b) = sprintf "(%d, %d)" a b in
    let%lwt () =
      S.iter db
        (fun a -> S.iter db (fun b -> Queue.push (a, b) q; return ()) inner)
        outer
    in
      aeq_list ~printer expected (Queue.fold (fun l x -> x :: l) [] q);
      let%lwt l =
        S.fold db
          (fun l a -> S.fold db (fun l b -> return ((a, b) :: l)) l inner)
          []
          outer
      in aeq_list ~printer expected l;
         return ()

  let test_nested_iter_and_fold () =
    (* nested iter/folds will spawn multiple Sqlexpr_sqlite_lwt workers, so
     * cannot use in-mem DB, lest the table not be created in other workers
     * than the one where it was created *)
    with_db ~in_mem:false do_test_nested_iter_and_fold ()

  let expect_missing_table tbl f =
    try%lwt
      f () >>= fun () ->
      assert_failure (sprintf "Expected Sqlite3.Error: missing table %s" tbl)
    with Sqlexpr_sqlite.Error _ -> return ()

  let test_borrow_worker () =
    with_db begin fun db () ->
      (* we borrow a worker repeatedly, but since we're doing everything
       * sequentially we end up using the same one all the time *)
      S.borrow_worker db
        (fun db' ->
           S.borrow_worker db (fun db'' -> do_test_nested_iter_and_fold db'' ()) >>= fun () ->
           nested_iter_and_fold_read db') >>= fun () ->
      nested_iter_and_fold_read db
    end ()

  let maybe_test flag f () =
    if flag then f () else return ()

  (* let test_borrow_worker has_real_borrow_worker () = *)
    (* if has_real_borrow_worker then test_borrow_worker () else return () *)

  let all_tests has_real_borrow_worker =
    [
      "Directives" >::: test_directives;
      "Outputs" >::: test_outputs;
      "Directives in output exprs" >:: test_oexpr_directives;
      "Transactions" >:: test_transaction;
      "Auto-retry BEGIN" >:: test_retry_begin;
      "Fold and iter" >:: test_fold_and_iter;
      "Nested fold and iter" >:: test_nested_iter_and_fold;
      "Borrow worker" >:: maybe_test has_real_borrow_worker test_borrow_worker;
    ]
end

let test_lwt_recursive_mutex () =
  let module M = Sqlexpr_concurrency.Lwt in
  let mv = Lwt_mvar.create () in
  let m = M.create_recursive_mutex () in
  let l = ref [] in
  let push x = l := x :: !l; return () in
  let%lwt n = M.with_lock m (fun () -> M.with_lock m (fun () -> return 42)) in
    aeq_int 42 n;
    let t1 = M.with_lock m (fun () -> push 1 >>= fun () -> Lwt_mvar.take mv >>= fun () -> push 2) in
    let t2 = M.with_lock m (fun () -> push 3) in
    let%lwt () = Lwt.join [ t1; t2; Lwt_mvar.put mv () ] in
      aeq_list ~printer:string_of_int [3; 2; 1] !l;
      return ()

module type S_LWT = Sqlexpr_sqlite.S with type 'a result = 'a Lwt.t

(* schema changes to :memory: db made by a Sqlexpr_sqlite_lwt worker are not
 * seen by the others, so allow to use a file by doing ~in_mem:false *)
let with_db
      (type a)
      (module S : S_LWT with type db = a)
      ?(in_mem = true) f x =
  let file =
    if in_mem then ":memory:" else Filename.temp_file "t_sqlexpr_sqlite_" "" in
  let db = S.open_db file in
  let%lwt () = try%lwt
      f db x
  with _ -> return () in
  S.close_db db;
  if not in_mem then Sys.remove file;
  return ()

let test_exclusion (type a)
      ((module S : S_LWT with type db = a) as s) () =
  let module Sqlexpr = S in
  with_db s ~in_mem:false begin fun db () ->
    S.execute db [%sql "CREATE TABLE foo(n INTEGER NOT NULL)"] >>= fun () ->

    let exclusion_between_tx_and_single_stmt () =
      let t1, u1 = Lwt.wait () in
      let t2, u2 = Lwt.wait () in
      let t3, u3 = Lwt.wait () in
      let th1    = S.transaction db
                     (fun db ->
                        t1 >|= Lwt.wakeup u2 >>= fun () ->
                        Lwt_unix.sleep 0.010 >>= fun () ->
                        S.select_one db [%sql "SELECT @d{COUNT(*)} FROM foo"] >|=
                          aeq_int ~msg:"number of rows (single stmt exclusion)" 0)
      and th2    = begin
                     t3 >>= fun () ->
                     S.execute db [%sql "INSERT INTO foo VALUES(1)"]
                   end
      and th3    = begin
                     Lwt.wakeup u1 ();
                     let%lwt () = t2 in
                       Lwt.wakeup u3 ();
                       return ()
                   end
      in
        th1 <&> th2 <&> th3 in

    let exclusion_between_txs () =
      let inside = ref 0 in
      let check db =
        let%lwt () = try%lwt
          incr inside;
          if !inside > 1 then
            assert_failure "More than one TX in critical region at a time";

          Lwt_unix.sleep 0.005
        with _ -> return () in
        decr inside;
        return ()
      in
        Lwt.join (Sqlexpr_utils.List.init 1 (fun _ -> S.transaction db check))
    in
      exclusion_between_txs () >>= fun () ->
      exclusion_between_tx_and_single_stmt ()
  end ()

module IdConc =
struct
  include Sqlexpr_concurrency.Id
  let iter = List.iter
  let run x = x
end

module LwtConc =
struct
  include Sqlexpr_concurrency.Lwt
  let run x = Lwt_unix.run (Lwt.pick [x; Lwt_unix.timeout 1.0])
  let iter = Lwt_list.iter_s
end

let lwt_run f () = LwtConc.run (f ())

let all_tests =
  [
    "Sqlexpr_concurrency.Lwt.with_lock" >:: lwt_run test_lwt_recursive_mutex;
    (let module M = Test(IdConc)(Sqlexpr_sqlite.Make(IdConc)) in
      "Sqlexpr_sqlite.Make(Sqlexpr_concurrency.Id)" >::: M.all_tests false);
    (let module M = Test(LwtConc)(Sqlexpr_sqlite.Make(LwtConc)) in
      "Sqlexpr_sqlite.Make(LwtConcurrency)" >::: M.all_tests false);
    (let module M = Test(LwtConc)(Sqlexpr_sqlite_lwt) in
      "Sqlexpr_sqlite_lwt" >::: M.all_tests true);
    "Sqlexpr_sqlite.Make(LwtConcurrency) exclusion" >::
      lwt_run (test_exclusion (module Sqlexpr_sqlite.Make(LwtConc)));
  ]

let _ =
  run_test_tt_main ("All" >::: all_tests)
