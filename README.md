# `rotonda-store`

In-memory IP prefixes storage and retrieval. Part of the `Rotonda` modular BGP engine.

A `rotonda-store` is a data structure that stores both IPv4 and IPv6 prefixes together with arbitrary
meta-data in a tree-bitmap[^1]. The tree-bitmap allows for fast querying of IP prefixes and their more- 
and less-specific prefixes.

This crate provides a data-structure intended for single-threaded use, and a data-structure for 
multi-threaded use.

[^1]: Read more about the data-structure in this [blog post](https://blog.nlnetlabs.nl/donkeys-mules-horses/).
