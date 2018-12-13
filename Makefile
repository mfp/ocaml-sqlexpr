all:
	dune build --root . @install @tests/runtest

clean:
	dune clean --root .
