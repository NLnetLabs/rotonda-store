# `rotonda-store`

An In-Memory Routing Information Base (`RIB`) for IPv4 and IPv6 Prefixes. Part
of the `Rotonda` modular BGP engine.

Although this store is geared towards storing routing information, it can
store any type of metadata for a prefix.

It features as secondary key a u32 value, which can be used to store multiple
values for one prefix, e.g. representing different peers, or add_path routes.

The built-in tree-bitmap[^1] allows for fast querying of IP prefixes and their
more- and less-specific prefixes.

This crate provides a data-structure intended for single-threaded use, and a
data-structure for  multi-threaded use.

[^1]: Read more about the data-structure in this [blog post](https://blog.nlnetlabs.nl/donkeys-mules-horses/).
