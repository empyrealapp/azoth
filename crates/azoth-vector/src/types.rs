//! Vector types and configuration

use serde::{Deserialize, Serialize};

/// Vector data types supported by sqlite-vector
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VectorType {
    /// 32-bit floating point (default)
    Float32,
    /// 16-bit floating point
    Float16,
    /// Brain floating point 16-bit
    BFloat16,
    /// 8-bit signed integer
    Int8,
    /// 8-bit unsigned integer
    UInt8,
    /// 1-bit binary vector
    Bit1,
}

impl std::fmt::Display for VectorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VectorType::Float32 => write!(f, "FLOAT32"),
            VectorType::Float16 => write!(f, "FLOAT16"),
            VectorType::BFloat16 => write!(f, "BFLOAT16"),
            VectorType::Int8 => write!(f, "INT8"),
            VectorType::UInt8 => write!(f, "UINT8"),
            VectorType::Bit1 => write!(f, "1BIT"),
        }
    }
}

impl Default for VectorType {
    fn default() -> Self {
        VectorType::Float32
    }
}

/// Distance metrics for similarity search
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DistanceMetric {
    /// Euclidean distance (L2 norm)
    L2,
    /// Squared Euclidean distance (faster, same ranking as L2)
    SquaredL2,
    /// Manhattan distance (L1 norm)
    L1,
    /// Cosine similarity (angle between vectors)
    Cosine,
    /// Dot product (for normalized vectors)
    DotProduct,
    /// Hamming distance (for 1-bit vectors)
    Hamming,
}

impl std::fmt::Display for DistanceMetric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DistanceMetric::L2 => write!(f, "l2"),
            DistanceMetric::SquaredL2 => write!(f, "squared_l2"),
            DistanceMetric::L1 => write!(f, "l1"),
            DistanceMetric::Cosine => write!(f, "cosine"),
            DistanceMetric::DotProduct => write!(f, "dot"),
            DistanceMetric::Hamming => write!(f, "hamming"),
        }
    }
}

impl Default for DistanceMetric {
    fn default() -> Self {
        DistanceMetric::Cosine
    }
}

/// Configuration for a vector column
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorConfig {
    /// Vector data type
    pub vector_type: VectorType,
    /// Vector dimension (number of elements)
    pub dimension: usize,
    /// Distance metric for similarity search
    pub distance_metric: DistanceMetric,
}

impl VectorConfig {
    /// Create a new vector configuration
    pub fn new(vector_type: VectorType, dimension: usize, distance_metric: DistanceMetric) -> Self {
        Self {
            vector_type,
            dimension,
            distance_metric,
        }
    }

    /// Convert to sqlite-vector config string
    pub(crate) fn to_config_string(&self) -> String {
        format!("type={},dimension={}", self.vector_type, self.dimension)
    }
}

impl Default for VectorConfig {
    fn default() -> Self {
        Self {
            vector_type: VectorType::Float32,
            dimension: 384, // Common dimension for sentence transformers
            distance_metric: DistanceMetric::Cosine,
        }
    }
}

/// A vector embedding
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vector {
    /// Vector data as f32 values
    pub data: Vec<f32>,
    /// Vector dimension
    pub dimension: usize,
}

impl Vector {
    /// Create a new vector from f32 data
    pub fn new(data: Vec<f32>) -> Self {
        let dimension = data.len();
        Self { data, dimension }
    }

    /// Serialize to BLOB for SQLite storage
    ///
    /// Stores as little-endian f32 values
    pub fn to_blob(&self) -> Vec<u8> {
        self.data
            .iter()
            .flat_map(|f| f.to_le_bytes())
            .collect()
    }

    /// Deserialize from BLOB
    pub fn from_blob(blob: &[u8]) -> Result<Self, String> {
        if blob.len() % 4 != 0 {
            return Err(format!(
                "Invalid blob length {} for f32 vector (must be multiple of 4)",
                blob.len()
            ));
        }

        let data = blob
            .chunks_exact(4)
            .map(|chunk| {
                let bytes = [chunk[0], chunk[1], chunk[2], chunk[3]];
                f32::from_le_bytes(bytes)
            })
            .collect::<Vec<_>>();

        Ok(Self::new(data))
    }

    /// Convert to JSON array (for queries)
    pub fn to_json(&self) -> String {
        serde_json::to_string(&self.data).unwrap()
    }

    /// Get vector dimension
    pub fn dimension(&self) -> usize {
        self.dimension
    }

    /// Get vector data as slice
    pub fn as_slice(&self) -> &[f32] {
        &self.data
    }
}

impl From<Vec<f32>> for Vector {
    fn from(data: Vec<f32>) -> Self {
        Self::new(data)
    }
}

/// Search result with distance/similarity score
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    /// Row ID of the result
    pub rowid: i64,
    /// Distance/similarity score
    ///
    /// For L2/L1/Hamming: lower is more similar
    /// For Cosine/Dot: higher is more similar (but sqlite-vector returns distance)
    pub distance: f32,
}

impl SearchResult {
    /// Create a new search result
    pub fn new(rowid: i64, distance: f32) -> Self {
        Self { rowid, distance }
    }

    /// Convert distance to similarity score for cosine/dot metrics
    ///
    /// sqlite-vector returns distance, but cosine similarity is more intuitive
    /// as a score where 1.0 = identical, 0.0 = orthogonal
    pub fn similarity(&self, metric: DistanceMetric) -> f32 {
        match metric {
            DistanceMetric::Cosine | DistanceMetric::DotProduct => 1.0 - self.distance,
            _ => self.distance,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_blob_roundtrip() {
        let original = Vector::new(vec![0.1, 0.2, 0.3, 0.4]);
        let blob = original.to_blob();
        let decoded = Vector::from_blob(&blob).unwrap();

        assert_eq!(original.dimension, decoded.dimension);
        for (a, b) in original.data.iter().zip(decoded.data.iter()) {
            assert!((a - b).abs() < 1e-6);
        }
    }

    #[test]
    fn test_vector_json() {
        let vector = Vector::new(vec![1.0, 2.0, 3.0]);
        let json = vector.to_json();
        assert_eq!(json, "[1.0,2.0,3.0]");
    }

    #[test]
    fn test_config_string() {
        let config = VectorConfig::new(VectorType::Float32, 384, DistanceMetric::Cosine);
        assert_eq!(config.to_config_string(), "type=FLOAT32,dimension=384");
    }

    #[test]
    fn test_invalid_blob() {
        let invalid_blob = vec![0u8, 1, 2]; // Not multiple of 4
        let result = Vector::from_blob(&invalid_blob);
        assert!(result.is_err());
    }
}
