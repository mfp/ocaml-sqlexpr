module Option : sig
  val may : ('a -> unit) -> 'a option -> unit
  val map : ('a -> 'b) -> 'a option -> 'b option
  val map_default : ('a -> 'b) -> 'b -> 'a option -> 'b
end

