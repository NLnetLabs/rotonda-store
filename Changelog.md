# Change Log

## Unreleased new version

Released yyyy-mm-dd.

Breaking Changes

New

Bug fixes

Other changes

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
