# Change Log

## 0.2.0

Released 2021-09-07

Breaking Changes

* Search functions have been replaced with one public `search_prefix` functions that requires
  an `MatchOptions` struct as argument, the type of search and the data to be included in the
  return value.

New

* Adds more specifics searches
* Adds `EmptyMatch` search type for prefixes that have no less-specifics, but do have
  more-specifics.

Other Changes

* Various small optimisations.
* Added tests for more-specifics.
* Broken out several `features`, `cli`, `dynamodb`, `csv` in order to minimize minimal required
  dependencies.

[more-specifics #4]: https://github.com/NLnetLabs/rotonda-store/pull/4

## 0.1.1

Release 2021-08-13

Bug Fixes

* Fix rotonda-store not compiling on on rustc > 1.51

## 0.1.0

Released 2021-07-08

Initial public, but informal, release.
