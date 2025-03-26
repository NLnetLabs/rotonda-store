use std::fmt;

/// Possible errors returned by methods on a RIB
#[derive(Debug, PartialEq, Eq)]
pub enum PrefixStoreError {
    /// There is too much contention while creating a node: the store has
    /// given up. The method or function returning this error can be safely
    /// retries.
    NodeCreationMaxRetryError,
    /// A node that does not exist (yet), maybe due to contention. The
    ///function or method causing this error can be safely retried.
    NodeNotFound,
    /// The method returning this error presupposes a condition that has not
    /// been met, and may never be met. Retrying is safe, but may result in
    /// the same error. Therefore it should probably be retried only once.
    StoreNotReadyError,
    /// A best path was requested, but the selection procedure was performed
    /// on a route set that is now stale. A new best path calculation over the
    /// set should be performed before retrying.
    PathSelectionOutdated,
    /// The requested prefix was not found in the store.
    PrefixNotFound,
    /// A best path was requested, but it was never calculated. Perform a best
    ///path selection first, before retrying.
    BestPathNotFound,
    /// A record was specifically requested from the in-memory data structure,
    /// but the record is not in memory. It may be persisted to disk.
    RecordNotInMemory,
    /// An error external to to our execution happened, most notably another
    ///thread panicking while trying to acquire a lock.
    ExternalError,
    /// The method returning this error was trying to persist records to disk
    /// but failed. Retrying is safe, but may be yield the same result.
    PersistFailed,
    /// A status for a record was requested, but it was never set.
    StatusUnknown,
}

impl std::error::Error for PrefixStoreError {}

impl fmt::Display for PrefixStoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PrefixStoreError::NodeCreationMaxRetryError => write!(
                f,
                "Error: Maximum number of retries for node creation reached."
            ),
            PrefixStoreError::NodeNotFound => {
                write!(f, "Error: Node not found.")
            }
            PrefixStoreError::StoreNotReadyError => {
                write!(f, "Error: Store isn't ready yet.")
            }
            PrefixStoreError::PathSelectionOutdated => {
                write!(
                    f,
                    "Error: The Path Selection process is based on \
                    outdated paths."
                )
            }
            PrefixStoreError::PrefixNotFound => {
                write!(f, "Error: The Prefix cannot be found.")
            }
            PrefixStoreError::BestPathNotFound => {
                write!(
                    f,
                    "Error: The Prefix does not have a stored best path."
                )
            }
            PrefixStoreError::RecordNotInMemory => {
                write!(
                    f,
                    "Error: The Record for this (prefix, mui) is not in \
                    memory."
                )
            }
            PrefixStoreError::PersistFailed => {
                write!(
                    f,
                    "Error: The record for this (prefix, mui) cannot be \
                    persisted."
                )
            }
            PrefixStoreError::ExternalError => {
                write!(
                f,
                "Error: An action could not be completed, due to another \
                thread panicking."
            )
            }
            PrefixStoreError::StatusUnknown => {
                write!(
                    f,
                    "Warning: The record is persisted, but the upsert \
                    counters cannot be reported for persist only strategy."
                )
            }
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct FatalError;

impl std::fmt::Display for FatalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Error: A Fatal error has occurred. The store must be considered \
             corrupted. The application should terminate."
        )
    }
}

pub type FatalResult<T> = Result<T, FatalError>;

impl std::error::Error for FatalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }

    fn description(&self) -> &str {
        "description() is deprecated; use Display"
    }

    fn cause(&self) -> Option<&dyn std::error::Error> {
        self.source()
    }
}
