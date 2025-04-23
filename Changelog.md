# Change Log


## 0.5.0

Released 2025-04-23.

Breaking Changes

  * Almost all of the public API has been reorganised on the module
    level, most notably the store is exposed through the `rib` module,
    which hosts the `StartCastRib` struct. The `StarCastRib` replaces the
    `CustomAllocStorage`.
  * Configuration is done through one of the structs in the `config` module.

New

  * Routes can now be persisted to disk, both current and historical.
  * The structs in the `config` module allow picking a `PersistStrategy`,
    that handles how routes are stored in the RIB, in memory, on disk, or a
    combination of both. They also regulate the storage of historical records.
  * Methods to handle disk storage, i.e. `flush_to_disk` for RIBs.
  * The `MatchOptions` has a new field `include_history`.

Bug fixes

  * Under certain circumstances existing more specific prefixes for a
    requested prefix were ignored. This is now fixed.

Other changes

  * the Treebitmap and the backing Chained Hash Tables (CHTs) have been
    completely separated logically.
  * All iterators have been thoroughly reworked and optmised to avoid looping
    and branching as much as possible.
  * Internally only a stride size of 4 is used for both the CHTs and the
    treebitmap. This speeds up hopping from stride to stride, since no table
    lookups have to be performed anymore. Also, stride size 4 turned out to
    be the most memory efficient layout. PrefixSet and NodeSet sizes in the
    CHTs remain max. 16. This allowed for:
  * The removal of almost all macros, increasing the readability of the code
    immensely.
    
Known Limitations

  * The persist to disk feature only works with manual `flush_to_disk` calls
    from the user currently.


## 0.4.1

Released 2025-01-29.

Bug fixes

* Incorrect Error message in case the status of a prefix was modified without it actually existing in the store, is now fixed. It was `StoreNotReady`, it has been changed to `PrefixNotFound`.

## 0.4.0

Released 2024-11-20.

Breaking changes

* Removed `MergeUpdate` trait

New

* Leaves now are HashMaps keyed on multi_uniq_ids (`mui`)
* Facilities for best (and backup) path selection for a prefix
* Facilities for iterating over and searching for values for (prefix, mui)
  combinations

Bug fixes

Other changes

* Use inetnum structs instead of routecore (Asn, Prefix)

## 0.3.0

Released 2024-01-18.

Breaking changes

* Upsert() returns user-defined data, as well as contention count.
* Minimal Rust Version bumped to 1.71
* Depend on routecore 0.4.0-rc0

New

* RecordSet::new method; allows users to create RecordSets.

Bug fixes

* Fixed: Debug build couldn't handle searches for 0/0. Release versions relied on UB.

Other changes

* Made some structs pub
* Added Copy derives to public types

## 0.2.0

Released 2021-09-07.

Breaking Changes

* Search functions have been replaced with one public `search_prefix` functions that requires
  an `MatchOptions` struct as argument, the type of search and the data to be included in the
  return value.

New

* Adds more specifics searches
* Adds `EmptyMatch` search type for prefixes that have no less-specifics, but do have
  more-specifics.

Other Changes

* Various small optimizations.
* Added tests for more-specifics.
* Broken out several `features`, `cli`, `dynamodb`, `csv` in order to minimize minimal required
  dependencies.

[more-specifics #4]: https://github.com/NLnetLabs/rotonda-store/pull/4

## 0.1.1

Release 2021-08-13.

Bug Fixes

* Fix `rotonda-store` not compiling on on `rustc` > 1.51

## 0.1.0

Released 2021-07-08

Initial public, but informal, release.
