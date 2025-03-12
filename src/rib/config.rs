//------------ Config --------------------------------------------------------

//! Configuration options for a RIB for AFI/SAFIs [IPv4, IPv6] with [Unicast,
//! Multicast]

/// Defines where records are stored, in-memory and/or persisted (to disk),
/// and, whether new records for a unique (prefix, mui) pair are overwritten
/// or persisted.
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
    fn persist_strategy(&self) -> PersistStrategy;
    fn persist_path(&self) -> Option<String>;
    fn set_persist_path(&mut self, path: String);
}

//------------ MemoryOnlyConfig ----------------------------------------------

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

//------------ PeristOnlyConfig ----------------------------------------------

impl Default for PersistOnlyConfig {
    fn default() -> Self {
        Self {
            persist_path: "/tmp/rotonda/".to_string(),
        }
    }
}

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

//------------ WriteAheadConfig ----------------------------------------------

impl Default for WriteAheadConfig {
    fn default() -> Self {
        Self {
            persist_path: "/tmp/rotonda/".to_string(),
        }
    }
}

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

//------------ PersistHistoryConfig ------------------------------------------

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
