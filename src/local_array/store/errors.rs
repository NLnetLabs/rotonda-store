use std::fmt;

#[derive(Debug)]
pub enum PrefixStoreError {
    NodeCreationMaxRetryError,
    NodeNotFound,
    StoreNotReadyError,
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
        }
    }
}
