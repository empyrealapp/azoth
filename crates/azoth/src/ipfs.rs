//! IPFS integration for Azoth backups
//!
//! Provides simple IPFS upload/download functionality with Pinata support.
//!
//! # Example
//!
//! ```no_run
//! use azoth::ipfs::{IpfsClient, IpfsProvider};
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Create client with Pinata provider
//! let provider = IpfsProvider::Pinata {
//!     api_key: std::env::var("PINATA_API_KEY")?,
//!     secret_key: std::env::var("PINATA_SECRET_KEY")?,
//!     gateway_url: None,
//! };
//! let client = IpfsClient::new(provider);
//!
//! // Upload file
//! let data = std::fs::read("backup.tar.zst.age")?;
//! let cid = client.upload(&data, Some("backup.tar.zst.age".to_string())).await?;
//! println!("Uploaded to IPFS: {}", cid);
//!
//! // Download file
//! let downloaded = client.download(&cid).await?;
//! std::fs::write("restored.tar.zst.age", downloaded)?;
//! # Ok(())
//! # }
//! ```

use crate::{AzothError, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// IPFS provider configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IpfsProvider {
    /// Gateway-only provider (read-only)
    Gateway { url: String },
    /// Pinata provider with API credentials
    Pinata {
        api_key: String,
        secret_key: String,
        gateway_url: Option<String>,
    },
}

impl IpfsProvider {
    /// Create provider from environment variables
    ///
    /// Looks for:
    /// - PINATA_API_KEY and PINATA_SECRET_KEY for Pinata provider
    /// - PINATA_GATEWAY_URL (optional, defaults to https://gateway.pinata.cloud/ipfs/)
    /// - IPFS_GATEWAY_URL for gateway-only provider
    pub fn from_env() -> Result<Self> {
        use std::env;

        // Try to get Pinata keys
        if let (Ok(api_key), Ok(secret_key)) =
            (env::var("PINATA_API_KEY"), env::var("PINATA_SECRET_KEY"))
        {
            let gateway_url = env::var("PINATA_GATEWAY_URL").ok();
            return Ok(Self::Pinata {
                api_key,
                secret_key,
                gateway_url,
            });
        }

        // Fall back to gateway-only
        let gateway_url = env::var("IPFS_GATEWAY_URL")
            .unwrap_or_else(|_| "https://gateway.pinata.cloud/ipfs/".to_string());
        Ok(Self::Gateway { url: gateway_url })
    }

    /// Get the gateway URL for this provider
    pub fn gateway_url(&self) -> &str {
        match self {
            Self::Gateway { url } => url,
            Self::Pinata { gateway_url, .. } => gateway_url
                .as_deref()
                .unwrap_or("https://gateway.pinata.cloud/ipfs/"),
        }
    }
}

/// IPFS client for uploading and downloading files
pub struct IpfsClient {
    provider: IpfsProvider,
    client: reqwest::Client,
}

impl IpfsClient {
    /// Create a new IPFS client with the given provider
    pub fn new(provider: IpfsProvider) -> Self {
        Self {
            provider,
            client: reqwest::Client::new(),
        }
    }

    /// Create a new IPFS client from environment variables
    pub fn from_env() -> Result<Self> {
        let provider = IpfsProvider::from_env()?;
        Ok(Self::new(provider))
    }

    /// Upload data to IPFS
    ///
    /// Returns the IPFS CID (content identifier) hash
    pub async fn upload(&self, data: &[u8], filename: Option<String>) -> Result<String> {
        let filename_str = filename.as_deref().unwrap_or("file").to_string();
        let data_size = data.len();
        let data_size_mb = data_size as f64 / 1_048_576.0;

        tracing::info!(
            "Starting IPFS upload: filename={}, size={} bytes ({:.2} MB)",
            filename_str,
            data_size,
            data_size_mb
        );

        let result = match &self.provider {
            IpfsProvider::Gateway { .. } => {
                return Err(AzothError::Config(
                    "IPFS uploads are not supported with gateway-only provider. \
                    Set PINATA_API_KEY and PINATA_SECRET_KEY to enable uploads."
                        .to_string(),
                ))
            }
            IpfsProvider::Pinata {
                api_key,
                secret_key,
                ..
            } => {
                self.upload_to_pinata(api_key, secret_key, data, filename)
                    .await
            }
        };

        match &result {
            Ok(hash) => {
                tracing::info!(
                    "IPFS upload completed: hash={}, filename={}, size={} bytes ({:.2} MB)",
                    hash,
                    filename_str,
                    data_size,
                    data_size_mb
                );
            }
            Err(e) => {
                tracing::error!(
                    "IPFS upload failed: filename={}, size={} bytes ({:.2} MB), error={}",
                    filename_str,
                    data_size,
                    data_size_mb,
                    e
                );
            }
        }

        result
    }

    /// Upload a file to IPFS
    pub async fn upload_file(&self, path: &Path) -> Result<String> {
        let data = std::fs::read(path)?;
        let filename = path
            .file_name()
            .and_then(|s| s.to_str())
            .map(|s| s.to_string());
        self.upload(&data, filename).await
    }

    /// Download data from IPFS by CID
    pub async fn download(&self, cid: &str) -> Result<Vec<u8>> {
        let gateway_url = self.provider.gateway_url();
        let url = if gateway_url.ends_with('/') {
            format!("{}{}", gateway_url, cid)
        } else {
            format!("{}/{}", gateway_url, cid)
        };

        tracing::info!("Downloading from IPFS: {}", url);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| AzothError::Backup(format!("Failed to download from IPFS: {}", e)))?;

        if !response.status().is_success() {
            return Err(AzothError::Backup(format!(
                "Failed to download from IPFS: status {}",
                response.status()
            )));
        }

        let data = response
            .bytes()
            .await
            .map_err(|e| AzothError::Backup(format!("Failed to read IPFS response: {}", e)))?
            .to_vec();

        tracing::info!("Downloaded {} bytes from IPFS", data.len());
        Ok(data)
    }

    /// Download from IPFS and save to file
    pub async fn download_to_file(&self, cid: &str, path: &Path) -> Result<()> {
        let data = self.download(cid).await?;
        std::fs::write(path, data)?;
        Ok(())
    }

    async fn upload_to_pinata(
        &self,
        api_key: &str,
        secret_key: &str,
        data: &[u8],
        filename: Option<String>,
    ) -> Result<String> {
        let filename_owned = filename.unwrap_or_else(|| "file".to_string());
        let url = "https://api.pinata.cloud/pinning/pinFileToIPFS";

        let mut form = reqwest::multipart::Form::new();

        let part = reqwest::multipart::Part::bytes(data.to_vec()).file_name(filename_owned);
        form = form.part("file", part);

        let pinata_options = serde_json::json!({
            "cidVersion": 1
        });
        form = form.text("pinataOptions", pinata_options.to_string());

        let response = self
            .client
            .post(url)
            .header("pinata_api_key", api_key)
            .header("pinata_secret_api_key", secret_key)
            .multipart(form)
            .send()
            .await
            .map_err(|e| AzothError::Backup(format!("Failed to upload to Pinata: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(AzothError::Backup(format!(
                "Pinata upload failed with status {}: {}",
                status, text
            )));
        }

        let json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| AzothError::Backup(format!("Failed to parse Pinata response: {}", e)))?;

        let hash = json["IpfsHash"]
            .as_str()
            .ok_or_else(|| {
                AzothError::Backup("Pinata response missing IpfsHash field".to_string())
            })?
            .to_string();

        Ok(hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provider_gateway_url() {
        let provider = IpfsProvider::Gateway {
            url: "https://example.com/ipfs/".to_string(),
        };
        assert_eq!(provider.gateway_url(), "https://example.com/ipfs/");

        let provider = IpfsProvider::Pinata {
            api_key: "key".to_string(),
            secret_key: "secret".to_string(),
            gateway_url: Some("https://my-gateway.com/".to_string()),
        };
        assert_eq!(provider.gateway_url(), "https://my-gateway.com/");

        let provider = IpfsProvider::Pinata {
            api_key: "key".to_string(),
            secret_key: "secret".to_string(),
            gateway_url: None,
        };
        assert_eq!(provider.gateway_url(), "https://gateway.pinata.cloud/ipfs/");
    }
}
