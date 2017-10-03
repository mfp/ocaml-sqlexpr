open OUnit

module Sqlexpr = Sqlexpr_sqlite.Make(Sqlexpr_concurrency.Id)
module S = Sqlexpr

let ae = assert_equal ~printer:(fun x -> x)

let pqi (d : ('a , 'b) S.statement) = d.S.sql_statement
let pq (d : ('a, 'b, 'c) S.expression) = pqi d.S.statement

let test_sql _ =

  let s = pqi [%sql "insert into values(%s{foo}, %s{bar})"] in
  ae "insert into values(?{foo}, ?{bar})" s;

  let s = pq [%sql "@d{kilroy} was @s{here}"] in
  ae "kilroy was here" s;

  (* dots in column names should be okay *)
  let s = pq [%sql "select @d{t1.id}, @s{t1.label} from table as t1 ..."] in
  ae "select t1.id, t1.label from table as t1 ..." s;

  (* verifies the order of regex substitution. output substitution pass runs
   * before the input pass to avoid injecting valid sqlexpr metacharacter '?'.
   * For example, given the following string, running the input pass first would
   * result in a valid sqlexpr string, leading to an incorrect substitiion in
   * the output pass. never mind that immediately adjacent inputs+outputs in
   *  valid SQL are extremely unlikely... *)
  let s = pqi [%sql "@s%d{abc}"] in
  ae "@s?{abc}" s;

  (* test invalid expressions and adjacencies *)
  let s = pq [%sql "@s@s %d@s{abc}%d@s%d@s%d{def}%d{ghi}@s"] in
  ae "@s@s ?abc?@s?@s?{def}?{ghi}@s" s;

  (* check that outputs are preserved, even if non-alphanumeric *)
  (* also check that whitespace is preserved *)
  let s = pq [%sql "@s{:kilroy}     @@was %@{here}"] in
  ae ":kilroy     @@was %@{here}" s;

  (* interpolation inside outputs should be allowed *)
  let s = pq [%sql "select @d{%d}"] in
  ae "select ?" s;

  (* allow some other non-alphanumeric characters *)
  let s = pq [%sql "select @d{COUNT(*)} FROM foo"] in
  ae "select COUNT(*) FROM foo" s;

  (* allow spaces inside outputs *)
  let s = pq [%sql "@d{count (*)}"] in
  ae "count (*)" s;

  let s = pqi [%sql "excellent"] in
  ae "excellent" s


let test_quotes _ =

  (* single quotes *)
  let s = pq [%sql "strftime('%s-%d', %s-%d @s{abc}%d{def} '@s{abc}%d{def}')"] in
  ae "strftime('%s-%d', ?-? abc?{def} '@s{abc}%d{def}')" s;

  (* double quotes *)
  let s = pq [%sql{|strftime("%s-%d", %s-%d @s{abc}%d{def} "@s{abc}%d{def}")|}] in
  ae {|strftime("%s-%d", ?-? abc?{def} "@s{abc}%d{def}")|} s;

  (* mixed quotes and nested quotes *)
  let s = pq [%sql {|@s{abc}"@s{def}"'@d{ghi}''%f'%f"%S"%S "'@s{jkl}%d'" '"%d'"|}] in
  ae {|abc"@s{def}"'@d{ghi}''%f'?"%S"? "'@s{jkl}%d'" '"%d'"|} s;

  (* more nested and unbalanced quotes *)
  let s = pqi [%sql {|"'%d'" %d '"%d"' "'%d"'|}] in
  ae {|"'%d'" ? '"%d"' "'%d"'|} s;

  (* allow a quoted 'column' name *)
  let s = pq [%sql {|@s{'hello'}|}] in
  ae {|'hello'|} s


let tests =
  "ppx_tests">::: [
    "test_sql">::test_sql;
    "test_quotes">::test_quotes;
  ]

let _ = run_test_tt_main tests
