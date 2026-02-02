//! On-chain registry storage backend for checkpoints
//!
//! Records backup metadata in an Ethereum smart contract while storing actual
//! backup data in another backend (IPFS, S3, etc.)
//!
//! # Smart Contract
//!
//! The BackupRegistry contract is deployed in `/contracts/backup-registry/`.
//! It provides a simple interface for registering and querying backups:
//!
//! ```solidity
//! function submitBackup(bytes32 backupId, string calldata ipfsHash) external
//! function getBackup(address user, bytes32 backupId) external view returns (string, uint256)
//! function getUserBackupIds(address user) external view returns (bytes32[])
//! ```
//!
//! # Example
//!
//! ```no_run
//! use azoth::onchain_registry::{OnChainRegistry, OnChainConfig};
//! use azoth::ipfs_storage::IpfsStorage;
//! use azoth::checkpoint::CheckpointManager;
//! use azoth::IpfsProvider;
//! use alloy_primitives::Address;
//! use std::str::FromStr;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Create IPFS storage backend
//! let ipfs = IpfsStorage::from_env()?;
//!
//! // Wrap with on-chain registry
//! let config = OnChainConfig {
//!     contract_address: Address::from_str("0x...")?,
//!     rpc_url: "https://sepolia.infura.io/v3/YOUR_KEY".to_string(),
//!     chain_id: 11155111, // Sepolia
//!     private_key: "0x...".to_string(),
//! };
//! let storage = OnChainRegistry::new(config, Box::new(ipfs)).await?;
//!
//! // Use with CheckpointManager
//! // let manager = CheckpointManager::new(db, Box::new(storage), checkpoint_dir);
//! # Ok(())
//! # }
//! ```

use crate::checkpoint::{CheckpointMetadata, CheckpointStorage};
use crate::{AzothError, Result};
use alloy_primitives::{Address, FixedBytes};
use alloy_provider::{Provider, ProviderBuilder, RootProvider};
use alloy_rpc_types::TransactionRequest;
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{sol, SolCall};
use alloy_transport_http::Http;
use async_trait::async_trait;
use reqwest::Client;
use std::path::Path;
use std::str::FromStr;

// Solidity function interfaces
sol! {
    function submitBackup(bytes32 backupId, string calldata ipfsHash) external;
    function getBackup(address user, bytes32 backupId) external view returns (string memory ipfsHash, uint256 timestamp);
    function getUserBackupIds(address user) external view returns (bytes32[] memory);
}

/// Configuration for on-chain registry
#[derive(Debug, Clone)]
pub struct OnChainConfig {
    /// BackupRegistry contract address
    pub contract_address: Address,
    /// RPC URL for Ethereum node
    pub rpc_url: String,
    /// Chain ID (1 = mainnet, 11155111 = sepolia, etc.)
    pub chain_id: u64,
    /// Private key for signing transactions (hex string with or without 0x prefix)
    pub private_key: String,
}

impl OnChainConfig {
    /// Create config from environment variables
    ///
    /// Looks for:
    /// - BACKUP_REGISTRY_ADDRESS
    /// - BACKUP_REGISTRY_RPC_URL
    /// - BACKUP_REGISTRY_CHAIN_ID
    /// - BACKUP_REGISTRY_PRIVATE_KEY or SERVER_WALLET_PRIVATE_KEY
    pub fn from_env() -> Result<Self> {
        use std::env;

        let contract_address = env::var("BACKUP_REGISTRY_ADDRESS")
            .map_err(|_| AzothError::Config("BACKUP_REGISTRY_ADDRESS not set".to_string()))?
            .parse()
            .map_err(|e| AzothError::Config(format!("Invalid contract address: {}", e)))?;

        let rpc_url = env::var("BACKUP_REGISTRY_RPC_URL")
            .map_err(|_| AzothError::Config("BACKUP_REGISTRY_RPC_URL not set".to_string()))?;

        let chain_id = env::var("BACKUP_REGISTRY_CHAIN_ID")
            .unwrap_or_else(|_| "11155111".to_string()) // Default to Sepolia
            .parse()
            .map_err(|e| AzothError::Config(format!("Invalid chain ID: {}", e)))?;

        let private_key = env::var("BACKUP_REGISTRY_PRIVATE_KEY")
            .or_else(|_| env::var("SERVER_WALLET_PRIVATE_KEY"))
            .map_err(|_| AzothError::Config("BACKUP_REGISTRY_PRIVATE_KEY not set".to_string()))?;

        Ok(Self {
            contract_address,
            rpc_url,
            chain_id,
            private_key,
        })
    }

    /// Sepolia testnet preset
    pub fn sepolia(contract_address: Address, rpc_url: String, private_key: String) -> Self {
        Self {
            contract_address,
            rpc_url,
            chain_id: 11155111,
            private_key,
        }
    }

    /// Ethereum mainnet preset
    pub fn mainnet(contract_address: Address, rpc_url: String, private_key: String) -> Self {
        Self {
            contract_address,
            rpc_url,
            chain_id: 1,
            private_key,
        }
    }
}

/// On-chain registry storage backend
///
/// Records backup metadata in a smart contract while storing actual
/// backup data in another backend (IPFS, S3, etc.)
pub struct OnChainRegistry {
    config: OnChainConfig,
    backup_storage: Box<dyn CheckpointStorage>,
    signer: PrivateKeySigner,
    provider: RootProvider<Http<Client>>,
}

impl OnChainRegistry {
    /// Create with config and underlying storage
    pub async fn new(config: OnChainConfig, storage: Box<dyn CheckpointStorage>) -> Result<Self> {
        // Parse private key
        let private_key = config.private_key.trim_start_matches("0x");
        let signer = PrivateKeySigner::from_str(private_key)
            .map_err(|e| AzothError::Config(format!("Invalid private key: {}", e)))?;

        // Create provider
        let provider = ProviderBuilder::new().on_http(
            config
                .rpc_url
                .parse()
                .map_err(|e| AzothError::Config(format!("Invalid RPC URL: {}", e)))?,
        );

        Ok(Self {
            config,
            backup_storage: storage,
            signer,
            provider,
        })
    }

    /// Create from environment variables
    pub async fn from_env(storage: Box<dyn CheckpointStorage>) -> Result<Self> {
        let config = OnChainConfig::from_env()?;
        Self::new(config, storage).await
    }

    /// Generate backup ID from checkpoint metadata
    ///
    /// Uses keccak256 hash of the checkpoint ID
    fn backup_id_from_metadata(metadata: &CheckpointMetadata) -> FixedBytes<32> {
        use alloy_primitives::keccak256;
        keccak256(metadata.id.as_bytes())
    }

    /// Get user address (derived from signer)
    fn user_address(&self) -> Address {
        self.signer.address()
    }
}

#[async_trait]
impl CheckpointStorage for OnChainRegistry {
    async fn upload(&self, path: &Path, metadata: &CheckpointMetadata) -> Result<String> {
        // 1. Upload to underlying storage (IPFS, S3, etc.)
        let cid = self.backup_storage.upload(path, metadata).await?;

        // 2. Register in smart contract
        let backup_id = Self::backup_id_from_metadata(metadata);

        tracing::info!(
            "Registering backup on-chain: id={}, cid={}, chain_id={}",
            metadata.id,
            cid,
            self.config.chain_id
        );

        // Encode the function call
        let call = submitBackupCall {
            backupId: backup_id,
            ipfsHash: cid.clone(),
        };
        let calldata = call.abi_encode();

        // Create transaction
        let mut tx = TransactionRequest::default()
            .to(self.config.contract_address)
            .input(calldata.into())
            .from(self.user_address());
        tx.chain_id = Some(self.config.chain_id);

        // Sign and send transaction
        let tx_hash = self
            .provider
            .send_transaction(tx)
            .await
            .map_err(|e| AzothError::Backup(format!("Failed to send transaction: {}", e)))?
            .watch()
            .await
            .map_err(|e| AzothError::Backup(format!("Transaction failed: {}", e)))?;

        tracing::info!(
            "Backup registered on-chain: tx={:?}, backup_id={:?}",
            tx_hash,
            backup_id
        );

        Ok(cid)
    }

    async fn download(&self, id: &str, path: &Path) -> Result<()> {
        // Download from underlying storage
        self.backup_storage.download(id, path).await
    }

    async fn delete(&self, id: &str) -> Result<()> {
        // Can't delete from blockchain, but can delete from underlying storage
        tracing::warn!(
            "Cannot delete from blockchain (immutable), deleting from underlying storage only"
        );
        self.backup_storage.delete(id).await
    }

    async fn list(&self) -> Result<Vec<CheckpointMetadata>> {
        // Query blockchain for registered backups
        let user = self.user_address();

        tracing::debug!("Querying on-chain backups for user: {:?}", user);

        // Call getUserBackupIds
        let call = getUserBackupIdsCall { user };
        let calldata = call.abi_encode();

        let tx = TransactionRequest::default()
            .to(self.config.contract_address)
            .input(calldata.into());

        let result = self
            .provider
            .call(&tx)
            .await
            .map_err(|e| AzothError::Backup(format!("Failed to query backup IDs: {}", e)))?;

        // Decode backup IDs
        let backup_ids = getUserBackupIdsCall::abi_decode_returns(&result, false)
            .map_err(|e| AzothError::Backup(format!("Failed to decode backup IDs: {}", e)))?
            ._0;

        tracing::debug!("Found {} backups on-chain", backup_ids.len());

        // For each backup ID, get the details
        let mut metadata_list = Vec::new();
        for backup_id in backup_ids {
            let call = getBackupCall {
                user,
                backupId: backup_id,
            };
            let calldata = call.abi_encode();

            let tx = TransactionRequest::default()
                .to(self.config.contract_address)
                .input(calldata.into());

            let result = self
                .provider
                .call(&tx)
                .await
                .map_err(|e| AzothError::Backup(format!("Failed to query backup: {}", e)))?;

            let decoded = getBackupCall::abi_decode_returns(&result, false)
                .map_err(|e| AzothError::Backup(format!("Failed to decode backup: {}", e)))?;

            let ipfs_hash = decoded.ipfsHash;
            let timestamp = decoded.timestamp;

            if !ipfs_hash.is_empty() {
                use chrono::{TimeZone, Utc};
                metadata_list.push(CheckpointMetadata {
                    id: ipfs_hash,
                    timestamp: Utc.timestamp_opt(timestamp.to::<i64>(), 0).unwrap(),
                    sealed_event_id: 0, // Not stored on-chain
                    size_bytes: 0,      // Not stored on-chain
                    name: None,
                    storage_type: "ipfs".to_string(),
                });
            }
        }

        Ok(metadata_list)
    }

    fn storage_type(&self) -> &str {
        "onchain"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_from_env() {
        use std::env;

        env::set_var(
            "BACKUP_REGISTRY_ADDRESS",
            "0x1234567890123456789012345678901234567890",
        );
        env::set_var(
            "BACKUP_REGISTRY_RPC_URL",
            "https://sepolia.infura.io/v3/test",
        );
        env::set_var("BACKUP_REGISTRY_CHAIN_ID", "11155111");
        env::set_var(
            "BACKUP_REGISTRY_PRIVATE_KEY",
            "0x1234567890123456789012345678901234567890123456789012345678901234",
        );

        let config = OnChainConfig::from_env().unwrap();
        assert_eq!(config.chain_id, 11155111);
        assert_eq!(config.rpc_url, "https://sepolia.infura.io/v3/test");

        env::remove_var("BACKUP_REGISTRY_ADDRESS");
        env::remove_var("BACKUP_REGISTRY_RPC_URL");
        env::remove_var("BACKUP_REGISTRY_CHAIN_ID");
        env::remove_var("BACKUP_REGISTRY_PRIVATE_KEY");
    }

    #[test]
    fn test_sepolia_preset() {
        use std::str::FromStr;

        let config = OnChainConfig::sepolia(
            Address::from_str("0x1234567890123456789012345678901234567890").unwrap(),
            "https://sepolia.infura.io/v3/test".to_string(),
            "0x1234567890123456789012345678901234567890123456789012345678901234".to_string(),
        );

        assert_eq!(config.chain_id, 11155111);
    }
}
