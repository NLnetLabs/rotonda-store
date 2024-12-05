use std::fmt;

#[derive(Debug, PartialEq, Eq)]
pub enum PrefixStoreError {
    NodeCreationMaxRetryError,
    NodeNotFound,
    StoreNotReadyError,
    PathSelectionOutdated,
    PrefixNotFound,
    BestPathNotFound,
    RecordNotInMemory,
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
                write!(f, "Error: The Path Selection process is based on outdated paths.")
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
        }
    }
}
