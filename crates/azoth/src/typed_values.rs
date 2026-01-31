//! Typed value support for the KV store
//!
//! Supports:
//! - Integer types (i8, i16, i32, i64, i128, u8, u16, u32, u64, u128, u256)
//! - Raw bytes
//! - Sets (with membership testing, add/remove)
//! - Arrays (with push/pop/insert operations)
//!
//! # Example
//!
//! ```no_run
//! use azoth::prelude::*;
//! use azoth::typed_values::{TypedValue, U256, Set, Array};
//!
//! # fn main() -> Result<()> {
//! let db = AzothDb::open("./data")?;
//!
//! Transaction::new(&db).execute(|ctx| {
//!     // Store a U256
//!     let value = U256::from(1000u64);
//!     ctx.set(b"balance", &TypedValue::U256(value))?;
//!
//!     // Store a Set
//!     let mut roles = Set::new();
//!     roles.insert("admin".to_string());
//!     roles.insert("user".to_string());
//!     ctx.set(b"roles", &TypedValue::Set(roles))?;
//!
//!     // Store an Array
//!     let history = Array::from(vec!["event1".to_string(), "event2".to_string()]);
//!     ctx.set(b"history", &TypedValue::Array(history))?;
//!
//!     Ok(())
//! })?;
//! # Ok(())
//! # }
//! ```

use crate::{AzothError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// 256-bit unsigned integer (placeholder - use uint::U256 in production)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct U256([u64; 4]);

impl U256 {
    pub fn zero() -> Self {
        Self([0; 4])
    }

    pub fn from_u64(value: u64) -> Self {
        Self([value, 0, 0, 0])
    }

    pub fn to_bytes(&self) -> [u8; 32] {
        let mut bytes = [0u8; 32];
        for (i, &word) in self.0.iter().enumerate() {
            bytes[i * 8..(i + 1) * 8].copy_from_slice(&word.to_le_bytes());
        }
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 32 {
            return Err(AzothError::Serialization(
                "U256 requires exactly 32 bytes".into(),
            ));
        }

        let mut words = [0u64; 4];
        for (i, word) in words.iter_mut().enumerate() {
            let start = i * 8;
            let end = start + 8;
            *word = u64::from_le_bytes(bytes[start..end].try_into().unwrap());
        }

        Ok(Self(words))
    }

    // Basic arithmetic (placeholder - implement full operations)
    pub fn checked_add(&self, _other: &Self) -> Option<Self> {
        // Simplified - real implementation would handle overflow
        Some(*self)
    }

    pub fn checked_sub(&self, _other: &Self) -> Option<Self> {
        // Simplified - real implementation would handle underflow
        Some(*self)
    }
}

impl From<u64> for U256 {
    fn from(value: u64) -> Self {
        Self::from_u64(value)
    }
}

/// 256-bit signed integer (placeholder)
pub type I256 = U256; // Simplified for now

/// Set type with membership operations
#[derive(Debug, Clone)]
pub struct Set<T: Eq + std::hash::Hash> {
    items: HashSet<T>,
}

impl<T: Serialize> Serialize for Set<T>
where
    T: Eq + std::hash::Hash,
{
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.items.serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for Set<T>
where
    T: Deserialize<'de> + Eq + std::hash::Hash,
{
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let items = HashSet::deserialize(deserializer)?;
        Ok(Set { items })
    }
}

impl<T: Eq + std::hash::Hash> Set<T> {
    pub fn new() -> Self {
        Self {
            items: HashSet::new(),
        }
    }

    pub fn insert(&mut self, item: T) -> bool {
        self.items.insert(item)
    }

    pub fn remove(&mut self, item: &T) -> bool {
        self.items.remove(item)
    }

    pub fn contains(&self, item: &T) -> bool {
        self.items.contains(item)
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.items.iter()
    }
}

impl<T: Eq + std::hash::Hash> Default for Set<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Array type with index operations
#[derive(Debug, Clone)]
pub struct Array<T> {
    items: Vec<T>,
}

impl<T: Serialize> Serialize for Array<T> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.items.serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for Array<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let items = Vec::deserialize(deserializer)?;
        Ok(Array { items })
    }
}

impl<T> Array<T> {
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    pub fn push(&mut self, item: T) {
        self.items.push(item);
    }

    pub fn pop(&mut self) -> Option<T> {
        self.items.pop()
    }

    pub fn insert(&mut self, index: usize, item: T) {
        self.items.insert(index, item);
    }

    pub fn remove(&mut self, index: usize) -> T {
        self.items.remove(index)
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        self.items.get(index)
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.items.iter()
    }
}

impl<T> Default for Array<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> From<Vec<T>> for Array<T> {
    fn from(items: Vec<T>) -> Self {
        Self { items }
    }
}

/// Typed value enum supporting all types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TypedValue {
    // Integer types
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    I128(i128),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
    U256(U256),
    I256(I256),

    // Raw bytes
    Bytes(Vec<u8>),

    // String
    String(String),

    // Collections
    Set(Set<String>), // String set for simplicity
    Array(Array<String>), // String array for simplicity

                      // TODO: Add typed sets/arrays if needed
                      // SetU64(Set<u64>),
                      // ArrayU64(Array<u64>),
}

impl TypedValue {
    /// Encode to bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| AzothError::Serialization(e.to_string()))
    }

    /// Decode from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).map_err(|e| AzothError::Serialization(e.to_string()))
    }

    /// Extract as U256
    pub fn as_u256(&self) -> Result<U256> {
        match self {
            TypedValue::U256(v) => Ok(*v),
            _ => Err(AzothError::InvalidState("Not a U256".into())),
        }
    }

    /// Extract as i64
    pub fn as_i64(&self) -> Result<i64> {
        match self {
            TypedValue::I64(v) => Ok(*v),
            TypedValue::I32(v) => Ok(*v as i64),
            TypedValue::I16(v) => Ok(*v as i64),
            TypedValue::I8(v) => Ok(*v as i64),
            _ => Err(AzothError::InvalidState("Not an i64".into())),
        }
    }

    /// Extract as Set
    pub fn as_set(&self) -> Result<&Set<String>> {
        match self {
            TypedValue::Set(s) => Ok(s),
            _ => Err(AzothError::InvalidState("Not a Set".into())),
        }
    }

    /// Extract as mutable Set
    pub fn as_set_mut(&mut self) -> Result<&mut Set<String>> {
        match self {
            TypedValue::Set(s) => Ok(s),
            _ => Err(AzothError::InvalidState("Not a Set".into())),
        }
    }

    /// Extract as Array
    pub fn as_array(&self) -> Result<&Array<String>> {
        match self {
            TypedValue::Array(a) => Ok(a),
            _ => Err(AzothError::InvalidState("Not an Array".into())),
        }
    }

    /// Extract as mutable Array
    pub fn as_array_mut(&mut self) -> Result<&mut Array<String>> {
        match self {
            TypedValue::Array(a) => Ok(a),
            _ => Err(AzothError::InvalidState("Not an Array".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u256_basics() {
        let value = U256::from_u64(1000);
        let bytes = value.to_bytes();
        let decoded = U256::from_bytes(&bytes).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_set_operations() {
        let mut set = Set::new();
        assert!(set.insert("admin".to_string()));
        assert!(set.insert("user".to_string()));
        assert!(!set.insert("admin".to_string())); // Duplicate

        assert!(set.contains(&"admin".to_string()));
        assert!(!set.contains(&"guest".to_string()));

        assert_eq!(set.len(), 2);

        assert!(set.remove(&"admin".to_string()));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_array_operations() {
        let mut array = Array::new();
        array.push("event1".to_string());
        array.push("event2".to_string());

        assert_eq!(array.len(), 2);
        assert_eq!(array.get(0), Some(&"event1".to_string()));

        array.insert(1, "inserted".to_string());
        assert_eq!(array.len(), 3);
        assert_eq!(array.get(1), Some(&"inserted".to_string()));

        let popped = array.pop();
        assert_eq!(popped, Some("event2".to_string()));
        assert_eq!(array.len(), 2);
    }

    #[test]
    fn test_typed_value_serialization() {
        let value = TypedValue::U64(12345);
        let bytes = value.to_bytes().unwrap();
        let decoded = TypedValue::from_bytes(&bytes).unwrap();

        match decoded {
            TypedValue::U64(v) => assert_eq!(v, 12345),
            _ => panic!("Wrong type"),
        }
    }

    #[test]
    fn test_typed_value_set() {
        let mut set = Set::new();
        set.insert("role1".to_string());
        set.insert("role2".to_string());

        let value = TypedValue::Set(set);
        let bytes = value.to_bytes().unwrap();
        let decoded = TypedValue::from_bytes(&bytes).unwrap();

        let decoded_set = decoded.as_set().unwrap();
        assert_eq!(decoded_set.len(), 2);
        assert!(decoded_set.contains(&"role1".to_string()));
    }
}
