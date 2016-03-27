
open Printf
open Camlp4.PreCast
open Pa_estring

type output_type =
  [ `Int | `Text | `Blob | `Float | `Int32 | `Int64 | `Bool]

type input_type = [output_type | `Any]

type no_output_element = [ `Literal of string | `Input of input_type * bool ]

type sql_element =
    [ no_output_element
    | `Output of no_output_element list * output_type * bool (* nullable *) ]

let collected_statements = ref []
let collected_init_statements = ref []

(* [parse_without_output_exprs continuation acc llist]
 * parse %x(?) and %%, but don't recognize @x{} expressions, passing a list
 * of no_output_elements to the continuation (used for open recursion). *)
let rec parse_without_output_exprs k acc = function
    Cons (_, '%', Cons (_, 'd', l)) -> do_parse_in k acc `Int l
  | Cons (_, '%', Cons (_, 'l', l)) -> do_parse_in k acc `Int32 l
  | Cons (_, '%', Cons (_, 'L', l)) -> do_parse_in k acc `Int64 l
  | Cons (_, '%', Cons (_, 's', l)) -> do_parse_in k acc `Text l
  | Cons (_, '%', Cons (_, 'S', l)) -> do_parse_in k acc `Blob l
  | Cons (_, '%', Cons (_, 'f', l)) -> do_parse_in k acc `Float l
  | Cons (_, '%', Cons (_, 'b', l)) -> do_parse_in k acc `Bool l
  | Cons (_, '%', Cons (_, 'a', l)) -> do_parse_in k acc `Any l
  | Cons (_, '%', Cons (_, '%', l)) -> begin
      match acc with
          `Literal s :: tl -> k (`Literal (s ^ "%") :: tl) l
        | tl -> k (`Literal "%" :: tl) l
    end
  | Cons (_, '%', Cons (loc, c, l)) ->
      Loc.raise loc (Failure (sprintf "Unknown input directive %C" c))
  | Cons (_, c, l) -> begin match acc with
        `Literal s :: tl -> k (`Literal (s ^ String.make 1 c) :: tl) l
      | tl -> k (`Literal (String.make 1 c) :: tl) l
    end
  | Nil _ -> List.rev acc

(* complete the `Input sql_element, recognizing the ? that indicates it's
 * nullable, if present *)
and do_parse_in k acc kind = function
  | Cons (_, '?', l) -> k (`Input (kind, true) :: acc) l
  | l -> k (`Input (kind, false) :: acc) l

(* @return list of [sql_elements] given a llist *)
let rec parse l : sql_element list = do_parse [] l

and do_parse acc l = parse_with_output_exprs acc l

(* like [parse_with_output_exprs] but also recognize @x{...}, returning
 * a list of [sql_element]s. Need not use open recursion here, because the
 * continuation will always be [do_parse]. *)
and parse_with_output_exprs acc = function
  | Cons (_, '@', Cons (_, 'd', l)) -> do_parse_out `Int acc l
  | Cons (_, '@', Cons (_, 'l', l)) -> do_parse_out `Int32 acc l
  | Cons (_, '@', Cons (_, 'L', l)) -> do_parse_out `Int64 acc l
  | Cons (_, '@', Cons (_, 's', l)) -> do_parse_out `Text acc l
  | Cons (_, '@', Cons (_, 'S', l)) -> do_parse_out `Blob acc l
  | Cons (_, '@', Cons (_, 'f', l)) -> do_parse_out `Float acc l
  | Cons (_, '@', Cons (_, 'b', l)) -> do_parse_out `Bool acc l
  | Cons (_, '@', Cons (_, '@', l)) -> begin match acc with
        `Literal s :: tl -> do_parse (`Literal (s ^ "@") :: tl) l
      | tl -> do_parse (`Literal "@" :: tl) l
    end
  | Cons (_, '@', Cons (loc, c, l)) ->
      Loc.raise loc (Failure (sprintf "Unknown output directive %C" c))
  | l -> parse_without_output_exprs do_parse acc l

(* read the trailing ? and { after a @x output expression delimiter, then read
 * the expression up to the next } *)
and do_parse_out kind acc = function
    Cons (_, '?', Cons (loc, '{', l)) ->
      read_expr acc loc true kind l
  | Cons (loc, '{', l) ->
      read_expr acc loc false kind l
  | Cons (loc, _, _) | Nil loc ->
      Loc.raise loc (Failure "Missing expression for output directive")

(* read the output expression up to the trailing '}'. Disallow output
 * expressions when parsing the inner expression. *)
and read_expr acc loc ?(text = "") nullable kind = function
    Cons (_, '}', l) ->
      let rec parse_output_expr acc l =
        parse_without_output_exprs parse_output_expr acc l in
      let elms : no_output_element list = parse_output_expr [] (unescape loc text) in
        do_parse (`Output (elms, kind, nullable) :: acc) l
  | Cons (_, c, l) -> read_expr acc loc ~text:(sprintf "%s%c" text c) nullable kind l
  | Nil _ ->
      Loc.raise loc (Failure "Unterminated output directive expression")

let new_id =
  let n = ref 0 in
    fun () ->
      incr n;
      sprintf "__pa_sql_%d" !n

let input_directive_id kind nullable =
  let s = match kind with
      `Int -> "int"
    | `Int32 -> "int32"
    | `Int64 -> "int64"
    | `Text -> "text"
    | `Blob -> "blob"
    | `Float -> "float"
    | `Bool -> "bool"
    | `Any -> "any"
  in if nullable then "maybe_" ^ s else s

let directive_expr ?(_loc = Loc.ghost) = function
    `Input (kind, nullable) ->
      let id = input_directive_id kind nullable in
        <:expr< Sqlexpr.Directives.$lid:id$ >>
  | `Literal s -> <:expr< Sqlexpr.Directives.literal $str:s$ >>

let sql_statement l =
  let b = Buffer.create 10 in
  let rec append_text = function
      `Input _ -> Buffer.add_char b '?'
    | `Literal s -> Buffer.add_string b s
  in
    List.iter append_text l;
    Buffer.contents b

let concat_map f l = List.concat (List.map f l)

let expand_output_elms = function
  | `Output (l, _, _) -> l
  | #no_output_element as d -> [d]

let create_sql_statement _loc ~cacheable sql_elms =
  let sql_elms = concat_map expand_output_elms sql_elms in
  let k = new_id () in
  let st = new_id () in
  let exp =
    List.fold_right
      (fun dir e -> <:expr< $directive_expr dir$ $e$ >>) sql_elms <:expr< $lid:k$ >> in
  let id =
    let signature =
      sprintf "%d-%f-%d-%S"
        (Unix.getpid ()) (Unix.gettimeofday ()) (Random.int 0x3FFFFFF)
        (sql_statement sql_elms)
    in Digest.to_hex (Digest.string signature) in
  let stmt_id =
    if cacheable then <:expr< Some $str:id$ >> else <:expr< None >>
  in
    <:expr<
      {
        Sqlexpr.sql_statement = $str:sql_statement sql_elms$;
        stmt_id = $stmt_id$;
        directive = (fun [$lid:k$ -> fun [$lid:st$ -> $exp$ $lid:st$]])
      } >>

let create_sql_expression _loc ~cacheable (sql_elms : sql_element list) =
  let statement = create_sql_statement _loc ~cacheable sql_elms in

  let conv_expr kind nullable e =
    let expr x =
      let name = (if nullable then "maybe_" else "") ^ x in
        <:expr< Sqlexpr.Conversion.$lid:name$ $e$ >>
    in
      match kind with
          `Int -> expr "int"
        | `Int32 -> expr "int32"
        | `Int64 -> expr "int64"
        | `Bool -> expr "bool"
        | `Float -> expr "float"
        | `Text -> expr "text"
        | `Blob -> expr "blob" in

  let id = new_id () in
  let conv_exprs =
    let n = ref 0 in
      concat_map
        (fun dir -> match dir with
             `Output (_, kind, nullable) ->
               let i = string_of_int !n in
                 incr n;
                 [ conv_expr kind nullable <:expr< $lid:id$.($int:i$) >> ]
           | _ -> [])
        sql_elms in
  let tuple_func =
    let e = match conv_exprs with
        [] -> assert false
      | [x] -> x
      | hd :: tl -> <:expr< ( $hd$, $Ast.exCom_of_list tl$ ) >>
    in <:expr< fun [$lid:id$ -> $e$] >>
  in
    <:expr<
      {
        Sqlexpr.statement = $statement$;
        get_data = ($int:string_of_int (List.length conv_exprs)$,
                    $tuple_func$);
      }
    >>

let expand_sql_literal ?(is_init = false) ~cacheable ctx _loc str =
  let sql_elms = parse (unescape _loc str) in
  let sql_stmt_text =
    let no_output = concat_map expand_output_elms sql_elms in
      sql_statement no_output in
  let push l = l := !l @ [sql_stmt_text] in
    push (if is_init then collected_init_statements else collected_statements);
    if List.exists (function `Output _ -> true | _ -> false) sql_elms then
      create_sql_expression _loc ~cacheable sql_elms
    else
      create_sql_statement _loc ~cacheable sql_elms

let string_list_expr ?(_loc = Loc.ghost) = function
    [] -> <:expr< [] >>
  | l ->
      List.fold_left
        (fun l e -> <:expr< [ $e$ :: $l$ ] >>)
        <:expr< [] >>
        (List.rev_map (fun s -> <:expr< $str:s$ >>) l)

let expand_sqlite_check_functions ctx _loc =
  let statement_check =
    <:expr<
      try
        ignore (Sqlite3.prepare db stmt)
      with [Sqlite3.Error s ->
              do {
                ret.val := False;
                Format.fprintf fmt "Error in statement %S: %s\n" stmt s
              }
      ]
    >> in
  let stmt_list = string_list_expr ~_loc !collected_statements in
  let check_in_db_expr =
    <:expr< fun db fmt ->
      let ret = ref True in
        do {
          List.iter (fun stmt -> $statement_check$) $stmt_list$;
          ret.val;
        }
    >> in
  let init_stmts = string_list_expr ~_loc !collected_init_statements in
  let init_db_expr =
    <:expr< fun db fmt ->
      let ret = ref True in
        do {
          List.iter
            (fun stmt ->
               match Sqlite3.exec db stmt with
                 [
                   Sqlite3.Rc.OK -> ()
                 | rc -> do {
                     ret.val := False;
                     Format.fprintf fmt "Error in init. SQL statement (%s)@ %S@\n"
                       (Sqlite3.errmsg db) stmt
                   }
                 ])
            $init_stmts$;
          ret.val
        } >> in
  let in_mem_check_expr =
    <:expr<
      fun fmt ->
      let db = Sqlite3.db_open ":memory:" in
        init_db db fmt && check_db db fmt
    >>
  in <:expr<
      let init_db = $init_db_expr$ in
      let check_db = $check_in_db_expr$ in
      let in_mem_check = $in_mem_check_expr$ in
        (init_db, check_db, in_mem_check)
  >>

let _ =
  Random.self_init ();
  register_expr_specifier "sql"
    (fun ctx _loc str -> expand_sql_literal ~cacheable:false ctx _loc str);
  register_expr_specifier "sqlinit"
    (fun ctx _loc str ->
       expand_sql_literal ~is_init:true ~cacheable:true ctx _loc str);
  register_expr_specifier "sqlc"
    (fun ctx _loc str ->
       let expr = expand_sql_literal ~cacheable:true ctx _loc str in
       let id = register_shared_expr ctx expr in
         <:expr< $id:id$ >>);
  register_expr_specifier "sql_check"
    (fun ctx _loc -> function
         "sqlite" -> expand_sqlite_check_functions ctx _loc
       | _ -> <:expr< () >>)

