//! IPFS storage backend for checkpoints

use crate::checkpoint::{CheckpointMetadata, CheckpointStorage};
use crate::ipfs::{IpfsClient, IpfsProvider};
use crate::{AzothError, Result};
use async_trait::async_trait;
use std::path::Path;

/// IPFS storage backend using Pinata or other IPFS providers
pub struct IpfsStorage {
    client: IpfsClient,
}

impl IpfsStorage {
    /// Create a new IPFS storage backend
    pub fn new(provider: IpfsProvider) -> Self {
        Self {
            client: IpfsClient::new(provider),
        }
    }

    /// Create from environment variables
    pub fn from_env() -> Result<Self> {
        let provider = IpfsProvider::from_env()?;
        Ok(Self::new(provider))
    }
}

#[async_trait]
impl CheckpointStorage for IpfsStorage {
    async fn upload(&self, path: &Path, _metadata: &CheckpointMetadata) -> Result<String> {
        // Upload to IPFS
        let cid = self.client.upload_file(path).await?;
        Ok(cid)
    }

    async fn download(&self, id: &str, path: &Path) -> Result<()> {
        // Download from IPFS using CID
        self.client.download_to_file(id, path).await?;
        Ok(())
    }

    async fn delete(&self, _id: &str) -> Result<()> {
        // IPFS doesn't support deletion (content-addressed storage)
        // Pinata API could unpin, but we keep it simple for now
        Err(AzothError::Config(
            "IPFS storage does not support deletion (content-addressed storage)".to_string(),
        ))
    }

    async fn list(&self) -> Result<Vec<CheckpointMetadata>> {
        // IPFS doesn't have a built-in list operation
        // Would need to maintain a separate index
        Err(AzothError::Config(
            "IPFS storage does not support listing (would need separate index)".to_string(),
        ))
    }

    fn storage_type(&self) -> &str {
        "ipfs"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ipfs_storage_creation() {
        let provider = IpfsProvider::Gateway {
            url: "https://gateway.pinata.cloud/ipfs/".to_string(),
        };
        let storage = IpfsStorage::new(provider);
        assert_eq!(storage.storage_type(), "ipfs");
    }
}
