//------------ Config --------------------------------------------------------

//! Configuration options for a RIB for AFI/SAFIs [IPv4, IPv6] with [Unicast,
//! Multicast].
//!
//! A Configuration is created by picking one of the `*Config` structs in
//! this module, instantiate it, set some fields on it, and pass it in as an
//! argument to [new_with_config](super::StarCastRib::new_with_config).
//!
//! ```
//! use rotonda_store::test_types::PrefixAs;
//! use rotonda_store::rib::StarCastRib;
//! use rotonda_store::rib::config::PersistOnlyConfig;
//!
//! let config = PersistOnlyConfig::default();
//! let tree_bitmap = StarCastRib::<PrefixAs, _>::new_with_config(config);
//! ```

/// Defines where records are stored: in-memory and/or persisted (to disk),
/// and, whether new records for a unique (prefix, mui) pair are overwritten
/// or persisted ("historical records").
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PersistStrategy {
    /// Current records are stored both in-memory and persisted. Historical
    /// records are persisted.
    WriteAhead,
    /// Current records are stored in-memory, historical records are
    /// persisted.
    PersistHistory,
    /// Current records are stored in-memory, historical records are discarded
    /// when newer records appear.
    MemoryOnly,
    /// Current records are persisted immediately. No records are stored in
    /// memory. Historical records are discarded when newer records appear.
    PersistOnly,
}

pub trait Config: Clone + Default + std::fmt::Debug {
    /// Returns the chosen persist strategy for this configuration
    fn persist_strategy(&self) -> PersistStrategy;
    /// Returns the path to the directory that is used to store persisted
    /// records
    fn persist_path(&self) -> Option<String>;
    /// Set the path to the directory that will be used to persist records to
    fn set_persist_path(&mut self, path: String);
}

//------------ MemoryOnlyConfig ----------------------------------------------

/// A configuration that sets persist strategy to
/// `PersistStrategy::MemoryOnly`: Records for unique `(prefix, mui)` pairs
/// are kept in memory, newer records will overwrite existing records. In
/// other words, no historical records are preserved.
#[derive(Copy, Clone, Debug)]
pub struct MemoryOnlyConfig;

impl Config for MemoryOnlyConfig {
    fn persist_strategy(&self) -> PersistStrategy {
        PersistStrategy::MemoryOnly
    }

    fn persist_path(&self) -> Option<String> {
        None
    }

    fn set_persist_path(&mut self, _: String) {
        unimplemented!()
    }
}

impl Default for MemoryOnlyConfig {
    fn default() -> Self {
        Self
    }
}

//------------ PersistOnlyConfig ---------------------------------------------

/// A configuration that sets the persist strategy to
/// `PersistStrategy::PersistOnly`: Records for unique `(prefix, mui)` pairs
/// are persisted to disk, newer records will overwrite existing records. In
/// other words, no historical records are preserved.
#[derive(Clone, Debug)]
pub struct PersistOnlyConfig {
    persist_path: String,
}

impl Config for PersistOnlyConfig {
    fn persist_strategy(&self) -> PersistStrategy {
        PersistStrategy::PersistOnly
    }

    fn persist_path(&self) -> Option<String> {
        Some(self.persist_path.clone())
    }

    fn set_persist_path(&mut self, path: String) {
        self.persist_path = path;
    }
}

impl Default for PersistOnlyConfig {
    fn default() -> Self {
        Self {
            persist_path: "/tmp/rotonda/".to_string(),
        }
    }
}

//------------ WriteAheadConfig ----------------------------------------------

/// A configuration that sets the persist strategy to
///`PersistStrategy::WriteAhead`: Records for unique `(prefix, mui)` pairs
///are both kept in memory and persisted to disk, newer records will overwrite
///existing records in memory, but all records will be kept persisted on disk.
///In other words, historical records are kept on disk.
#[derive(Clone, Debug)]
pub struct WriteAheadConfig {
    persist_path: String,
}

impl Config for WriteAheadConfig {
    fn persist_strategy(&self) -> PersistStrategy {
        PersistStrategy::WriteAhead
    }

    fn persist_path(&self) -> Option<String> {
        Some(self.persist_path.clone())
    }

    fn set_persist_path(&mut self, path: String) {
        self.persist_path = path;
    }
}

impl Default for WriteAheadConfig {
    fn default() -> Self {
        Self {
            persist_path: "/tmp/rotonda/".to_string(),
        }
    }
}

//------------ PersistHistoryConfig ------------------------------------------

/// A configuration that sets the persist strategy to
///`PersistStrategy::PersistHistory`: Records for unique `(prefix, mui)` pairs
///are kept in memory,newer records will replace existing records, but the
///existing records will be persisted to disk.
#[derive(Clone, Debug)]
pub struct PersistHistoryConfig {
    persist_path: String,
}

impl Config for PersistHistoryConfig {
    fn persist_strategy(&self) -> PersistStrategy {
        PersistStrategy::PersistHistory
    }

    fn persist_path(&self) -> Option<String> {
        Some(self.persist_path.clone())
    }

    fn set_persist_path(&mut self, path: String) {
        self.persist_path = path;
    }
}

impl Default for PersistHistoryConfig {
    fn default() -> Self {
        Self {
            persist_path: "/tmp/rotonda/".to_string(),
        }
    }
}
