type typ = Int | Int32 | Int64 | Float | Text | Blob | Bool | Any
type input = typ * bool
type output = string * typ * bool

let str2typ = function
  | "d" -> Int
  | "l" -> Int32
  | "L" -> Int64
  | "f" -> Float
  | "s" -> Text
  | "S" -> Blob
  | "b" -> Bool
  | "a" -> Any
  | _ -> failwith "Invalid type"

let typ2str = function
  | Int -> "int"
  | Int32 -> "int32"
  | Int64 -> "int64"
  | Float -> "float"
  | Text -> "text"
  | Blob -> "blob"
  | Bool -> "bool"
  | Any -> "any"

let in_type2str ((typ, optional) : input) =
  let typ = typ2str typ in
  if optional then "maybe_" ^ typ else typ

let out_type2str ((_, typ, optional) : output) =
  in_type2str (typ, optional)

let parse str =
  (* grievous hack to escape everything within quotes *)
  (* what about ignoring \' or \" ?       " *)
  (* manually escape "" because {| ... |} notation breaks syntax highlighting *)
  let escrgx = Re_pcre.regexp "('[^']*')|(\"[^\"]*\")" in
  let esc_list = ref [] in
  let esc_str = "<SQLEXPR_PRESERVED>" in
  let esc_subst substrings =
    let mtch = Re.get substrings 0 in
    esc_list := mtch :: !esc_list;
    esc_str in

  let escaped = Re.replace ~f:esc_subst escrgx str in
  esc_list := List.rev !esc_list;

  (* logic to extract inputs and outputs *)
  let inrgx = Re_pcre.regexp {|%([dlLfsSba])(\?)?|} in
  let outrgx = Re_pcre.regexp {|@([dlLfsSba])(\?)?\{([^}]+)\}|} in
  let getin (acc : input list) s =
    let groups = Re.get_all s in
    let typ = Array.get groups 1 |> str2typ in
    let optional = "?" = Array.get groups 2 in
    let res = typ, optional in
    res::acc in
  let getout (acc : output list) s =
    let groups = Re.get_all s in
    let typ = Array.get groups 1 |> str2typ in
    let optional = "?" = Array.get groups 2 in
    let name = Array.get groups 3 |> String.trim in
    let res = name, typ, optional in
    res::acc in

  (* execute extractions *)
  let ins = Re.all inrgx escaped |> List.fold_left getin [] |> List.rev in
  let outs = Re.all outrgx escaped |> List.fold_left getout [] |> List.rev in

  (* replace input and output params with regular SQL *)
  let in_subst substrs = "?" in

  let rep_count_out = ref 0 in
  let out_subst substrs =
    let (name, _,_) = List.nth outs !rep_count_out in
    incr rep_count_out;
    name in

  (* now restore the escaped strings *)
  let rep_esc_count = ref 0 in
  let unesc_subst substrs =
    let restore = List.nth !esc_list !rep_esc_count in
    incr rep_esc_count;
    restore in

  (* generate final sql *)
  let sql =
       Re.replace ~f:out_subst outrgx escaped
    |> Re.replace ~f:in_subst inrgx
    |> Re.replace ~f:unesc_subst (Re_pcre.regexp esc_str) in

  (* final return *)
  (sql, ins, outs)
