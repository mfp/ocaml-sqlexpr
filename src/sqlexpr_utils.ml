

module Option = struct
  let may f = function None -> () | Some x -> f x
  let map f = function None -> None | Some x -> Some (f x)
  let map_default f d o = match o with
    | None -> d
    | Some x -> f x 
end

module List = struct
  let init n f =
    let rec init i =
      if i=n then []
      else f i :: init (i+1)
    in init 0
end
