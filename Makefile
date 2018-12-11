all:
	dune build @install @tests/runtest

clean:
	dune clean
