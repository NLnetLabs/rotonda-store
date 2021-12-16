# Generating an input file

Some of the examples expect an input file of the form `<dotted quad IPv4 address>,<prefix length>,<AS number>`, e.g.:

```
104.160.161.0,24,46844
```

One way to generate such an input file is as follows:

```
$ curl https://www.ris.ripe.net/dumps/riswhoisdump.IPv4.gz -o - | \
  gunzip - | \
  awk '/^[^% ]/ {split($2,a,"/"); printf "%s,%d,%d\n", a[1], a[2], $1}' \
  > in.csv
```

# Running the examples

The examples in this directory can be run using a command of the form:

```
$ cargo run --release --all-features --example <EXAMPLE_NAME> [-- <path/to/input.csv>]
```

Where:
  - `<EXAMPLE_NAME>` should be replaced by the name of the example to run.
  - `[-- <path/to/input.csv>]` is only relevant for examples that take an input file.

Assuming that you have an appropriate version of the Rust compiler [installed](https://rustup.rs/), and that we had previously created an input file in the current directory called `in.csv`, then you could for instance run the `numbers_treebitmap` example like so:

```
$ cargo run --release --all-features --example numbers_treebitmap -- in.csv
```