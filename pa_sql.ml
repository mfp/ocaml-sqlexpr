
open Printf
open Camlp4.PreCast
open Pa_estring

type output_type =
  [ `Int | `Text | `Blob | `Float | `Int32 | `Int64 | `Bool]

type input_type = [output_type | `Any]

type element =
    Literal of string
  | Input of input_type
  | Output of string * output_type * bool (* nullable *)

let rec parse l = do_parse [] l

and do_parse acc = function
    Cons (_, '%', Cons (_, 'd', l)) -> do_parse (Input `Int :: acc) l
  | Cons (_, '%', Cons (_, 'l', l)) -> do_parse (Input `Int32 :: acc) l
  | Cons (_, '%', Cons (_, 'L', l)) -> do_parse (Input `Int64 :: acc) l
  | Cons (_, '%', Cons (_, 's', l)) -> do_parse (Input `Text :: acc) l
  | Cons (_, '%', Cons (_, 'S', l)) -> do_parse (Input `Blob :: acc) l
  | Cons (_, '%', Cons (_, 'f', l)) -> do_parse (Input `Float :: acc) l
  | Cons (_, '%', Cons (_, 'b', l)) -> do_parse (Input `Bool :: acc) l
  | Cons (_, '%', Cons (_, 'a', l)) -> do_parse (Input `Any :: acc) l
  | Cons (_, '%', Cons (_, '%', l)) -> begin
      match acc with
          Literal s :: tl -> do_parse (Literal (s ^ "%") :: tl) l
        | tl -> do_parse (Literal "%" :: tl) l
    end
  | Cons (_, '%', Cons (loc, c, l)) ->
      Loc.raise loc (Failure (sprintf "Unknown input directive %C" c))
  | Cons (_, '@', Cons (_, 'd', l)) -> do_parse_out `Int acc l
  | Cons (_, '@', Cons (_, 'l', l)) -> do_parse_out `Int32 acc l
  | Cons (_, '@', Cons (_, 'L', l)) -> do_parse_out `Int64 acc l
  | Cons (_, '@', Cons (_, 's', l)) -> do_parse_out `Text acc l
  | Cons (_, '@', Cons (_, 'S', l)) -> do_parse_out `Blob acc l
  | Cons (_, '@', Cons (_, 'f', l)) -> do_parse_out `Float acc l
  | Cons (_, '@', Cons (_, '@', l)) -> begin match acc with
        Literal s :: tl -> do_parse (Literal (s ^ "@") :: tl) l
      | tl -> do_parse (Literal "@" :: tl) l
    end
  | Cons (_, '@', Cons (loc, c, l)) ->
      Loc.raise loc (Failure (sprintf "Unknown output directive %C" c))
  | Cons (_, c, l) -> begin match acc with
        Literal s :: tl -> do_parse (Literal (s ^ String.make 1 c) :: tl) l
      | tl -> do_parse (Literal (String.make 1 c) :: tl) l
    end
  | Nil _ -> List.rev acc

and do_parse_out kind acc = function
    Cons (_, '?', Cons (loc, '{', l)) ->
      read_expr acc loc true kind l
  | Cons (loc, '{', l) ->
      read_expr acc loc false kind l
  | Cons (loc, _, _) | Nil loc ->
      Loc.raise loc (Failure "Missing expression for output directive")

and read_expr acc loc ?(name = "") nullable kind = function
    Cons (_, '}', l) -> do_parse (Output (name, kind, nullable) :: acc) l
  | Cons (_, c, l) -> read_expr acc loc ~name:(sprintf "%s%c" name c) nullable kind l
  | Nil _ ->
      Loc.raise loc (Failure "Unterminated output directive expression")

let new_id =
  let n = ref 0 in
    fun () ->
      incr n;
      sprintf "__pa_sql_%d" !n

let directive_expr ?(_loc = Loc.ghost) = function
    Input `Int -> <:expr< Sqlexpr.Directives.int >>
  | Input `Int32 -> <:expr< Sqlexpr.Directives.int32 >>
  | Input `Int64 -> <:expr< Sqlexpr.Directives.int64 >>
  | Input `Text -> <:expr< Sqlexpr.Directives.text >>
  | Input `Blob -> <:expr< Sqlexpr.Directives.blob >>
  | Input `Float -> <:expr< Sqlexpr.Directives.float >>
  | Input `Bool -> <:expr< Sqlexpr.Directives.bool >>
  | Input `Any -> <:expr< Sqlexpr.Directives.any >>
  | Literal s -> <:expr< Sqlexpr.Directives.literal $str:s$ >>
  | Output _ -> assert false

let sql_statement l =
  let b = Buffer.create 10 in
    List.iter
      (function
           Input _ -> Buffer.add_char b '?'
         | Literal s | Output (s, _, _) -> Buffer.add_string b s)
      l;
    Buffer.contents b

let create_sql_statement _loc ~cacheable sql =
  let k = new_id () in
  let st = new_id () in
  let exp =
    List.fold_right
      (fun dir e -> <:expr< $directive_expr dir$ $e$ >>) sql <:expr< $lid:k$ >> in
  let cacheable = if cacheable then <:expr< True >> else <:expr< False >> in
    <:expr<
      Sqlexpr.make_statement ~cacheable:$cacheable$
      $str:sql_statement sql$
      (fun [$lid:k$ -> fun [$lid:st$ -> $exp$ $lid:st$]]) >>

let create_sql_expression _loc ~cacheable sql =
  let statement =
    create_sql_statement _loc ~cacheable
      (List.map (function Output (s, _, _) -> Literal s | d -> d) sql) in

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
      List.map
        (fun dir -> match dir with
             Output (_, kind, nullable) ->
               let i = string_of_int !n in
                 incr n;
                 conv_expr kind nullable <:expr< $lid:id$.($int:i$) >>
           | _ -> assert false)
        (List.filter (function Output _ -> true | _ -> false) sql) in
  let tuple_func =
    let e = match conv_exprs with
        [] -> assert false
      | [x] -> x
      | hd :: tl -> <:expr< ( $hd$, $Ast.exCom_of_list tl$ ) >>
    in <:expr< fun [$lid:id$ -> $e$] >>
  in
    <:expr<
      Sqlexpr.make_expression
        $statement$
        $int:string_of_int (List.length conv_exprs)$
        $tuple_func$ >>

let expand_sql_literal ~cacheable ctx _loc str =
  let sql = parse (unescape _loc str) in
    if List.exists (function Output _ -> true | _ -> false) sql then
      create_sql_expression _loc ~cacheable sql
    else
      create_sql_statement _loc ~cacheable sql

let _ =
  register_expr_specifier "sql"
    (fun ctx _loc str -> expand_sql_literal ~cacheable:false ctx _loc str);
  register_expr_specifier "sqlc"
    (fun ctx _loc str ->
       let expr = expand_sql_literal ~cacheable:true ctx _loc str in
       let id = register_shared_expr ctx expr in
         <:expr< $id:id$ >>)

