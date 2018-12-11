module Option = struct
  let may f = function None -> () | Some x -> f x
  let map f = function None -> None | Some x -> Some (f x)
  let map_default f d o = match o with
    | None -> d
    | Some x -> f x
end
