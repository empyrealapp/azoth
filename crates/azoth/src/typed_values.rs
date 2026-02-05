//! Typed value support for the KV store
//!
//! Supports:
//! - Integer types (i8, i16, i32, i64, i128, u8, u16, u32, u64, u128, u256)
//! - Raw bytes
//! - String values
//! - Typed sets (String, i64, u64, u128, `Vec<u8>`) with membership testing
//! - Typed arrays (String, i64, u64, u128, `Vec<u8>`) with push/pop/insert operations
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
//!     // Store a String Set
//!     let mut roles = Set::new();
//!     roles.insert("admin".to_string());
//!     roles.insert("user".to_string());
//!     ctx.set(b"roles", &TypedValue::Set(roles))?;
//!
//!     // Store a typed u64 Set
//!     let mut user_ids = Set::new();
//!     user_ids.insert(100u64);
//!     user_ids.insert(200u64);
//!     ctx.set(b"user_ids", &TypedValue::SetU64(user_ids))?;
//!
//!     // Store a String Array
//!     let history = Array::from(vec!["event1".to_string(), "event2".to_string()]);
//!     ctx.set(b"history", &TypedValue::Array(history))?;
//!
//!     // Store a typed i64 Array
//!     let scores = Array::from(vec![-100i64, 0, 100]);
//!     ctx.set(b"scores", &TypedValue::ArrayI64(scores))?;
//!
//!     Ok(())
//! })?;
//! # Ok(())
//! # }
//! ```

use crate::{AzothError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// 256-bit unsigned integer stored as 4 u64 words in little-endian order
///
/// Note: PartialOrd/Ord are manually implemented to compare from most-significant
/// limb to least-significant, which is correct for numeric ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct U256([u64; 4]);

impl PartialOrd for U256 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for U256 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare from most-significant limb (index 3) to least-significant (index 0)
        // This is the correct ordering for little-endian storage
        for i in (0..4).rev() {
            match self.0[i].cmp(&other.0[i]) {
                std::cmp::Ordering::Equal => continue,
                ord => return ord,
            }
        }
        std::cmp::Ordering::Equal
    }
}

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

    /// Add two U256 values, returning None on overflow
    #[allow(clippy::needless_range_loop)]
    pub fn checked_add(&self, other: &Self) -> Option<Self> {
        let mut result = [0u64; 4];
        let mut carry = 0u128;

        for i in 0..4 {
            let sum = self.0[i] as u128 + other.0[i] as u128 + carry;
            result[i] = sum as u64;
            carry = sum >> 64;
        }

        // If there's still a carry after the last word, we overflowed
        if carry != 0 {
            None
        } else {
            Some(Self(result))
        }
    }

    /// Subtract two U256 values, returning None on underflow
    #[allow(clippy::needless_range_loop)]
    pub fn checked_sub(&self, other: &Self) -> Option<Self> {
        let mut result = [0u64; 4];
        let mut borrow = 0i128;

        for i in 0..4 {
            let diff = self.0[i] as i128 - other.0[i] as i128 - borrow;
            if diff < 0 {
                result[i] = (diff + (1i128 << 64)) as u64;
                borrow = 1;
            } else {
                result[i] = diff as u64;
                borrow = 0;
            }
        }

        // If there's still a borrow after the last word, we underflowed
        if borrow != 0 {
            None
        } else {
            Some(Self(result))
        }
    }
}

impl From<u64> for U256 {
    fn from(value: u64) -> Self {
        Self::from_u64(value)
    }
}

/// 256-bit signed integer
///
/// Note: Currently aliased to U256. This means signed operations are not properly
/// supported and negative values will be treated as large positive values.
/// A proper two's complement implementation is needed for production use.
pub type I256 = U256;

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

    // Collections - String
    Set(Set<String>),
    Array(Array<String>),

    // Collections - Integer types
    SetI64(Set<i64>),
    SetU64(Set<u64>),
    SetU128(Set<u128>),
    ArrayI64(Array<i64>),
    ArrayU64(Array<u64>),
    ArrayU128(Array<u128>),

    // Collections - Bytes
    SetBytes(Set<Vec<u8>>),
    ArrayBytes(Array<Vec<u8>>),
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

    /// Create a TypedValue from a JSON-serializable type
    ///
    /// Stores the value as JSON bytes in a Bytes variant.
    /// This is useful for storing domain types (structs, enums) in the KV store.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use azoth::TypedValue;
    /// use serde::{Serialize, Deserialize};
    ///
    /// #[derive(Serialize, Deserialize)]
    /// struct User {
    ///     id: String,
    ///     name: String,
    /// }
    ///
    /// let user = User {
    ///     id: "user123".to_string(),
    ///     name: "Alice".to_string(),
    /// };
    ///
    /// let value = TypedValue::from_json(&user).unwrap();
    /// ```
    pub fn from_json<T: Serialize>(value: &T) -> Result<Self> {
        let bytes =
            serde_json::to_vec(value).map_err(|e| AzothError::Serialization(e.to_string()))?;
        Ok(TypedValue::Bytes(bytes))
    }

    /// Extract a JSON-serializable type from a TypedValue
    ///
    /// Works with Bytes variant containing JSON data.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use azoth::TypedValue;
    /// use serde::{Serialize, Deserialize};
    ///
    /// #[derive(Serialize, Deserialize, Debug, PartialEq)]
    /// struct User {
    ///     id: String,
    ///     name: String,
    /// }
    ///
    /// let user = User {
    ///     id: "user123".to_string(),
    ///     name: "Alice".to_string(),
    /// };
    ///
    /// let value = TypedValue::from_json(&user).unwrap();
    /// let decoded: User = value.to_json().unwrap();
    /// assert_eq!(decoded, user);
    /// ```
    pub fn to_json<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        match self {
            TypedValue::Bytes(bytes) => {
                serde_json::from_slice(bytes).map_err(|e| AzothError::Serialization(e.to_string()))
            }
            _ => Err(AzothError::InvalidState(
                "TypedValue must be Bytes variant for JSON deserialization".into(),
            )),
        }
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

    /// Extract as `Set<i64>`
    pub fn as_set_i64(&self) -> Result<&Set<i64>> {
        match self {
            TypedValue::SetI64(s) => Ok(s),
            _ => Err(AzothError::InvalidState("Not a Set<i64>".into())),
        }
    }

    /// Extract as mutable `Set<i64>`
    pub fn as_set_i64_mut(&mut self) -> Result<&mut Set<i64>> {
        match self {
            TypedValue::SetI64(s) => Ok(s),
            _ => Err(AzothError::InvalidState("Not a Set<i64>".into())),
        }
    }

    /// Extract as `Set<u64>`
    pub fn as_set_u64(&self) -> Result<&Set<u64>> {
        match self {
            TypedValue::SetU64(s) => Ok(s),
            _ => Err(AzothError::InvalidState("Not a Set<u64>".into())),
        }
    }

    /// Extract as mutable `Set<u64>`
    pub fn as_set_u64_mut(&mut self) -> Result<&mut Set<u64>> {
        match self {
            TypedValue::SetU64(s) => Ok(s),
            _ => Err(AzothError::InvalidState("Not a Set<u64>".into())),
        }
    }

    /// Extract as `Set<u128>`
    pub fn as_set_u128(&self) -> Result<&Set<u128>> {
        match self {
            TypedValue::SetU128(s) => Ok(s),
            _ => Err(AzothError::InvalidState("Not a Set<u128>".into())),
        }
    }

    /// Extract as mutable `Set<u128>`
    pub fn as_set_u128_mut(&mut self) -> Result<&mut Set<u128>> {
        match self {
            TypedValue::SetU128(s) => Ok(s),
            _ => Err(AzothError::InvalidState("Not a Set<u128>".into())),
        }
    }

    /// Extract as `Array<i64>`
    pub fn as_array_i64(&self) -> Result<&Array<i64>> {
        match self {
            TypedValue::ArrayI64(a) => Ok(a),
            _ => Err(AzothError::InvalidState("Not an Array<i64>".into())),
        }
    }

    /// Extract as mutable `Array<i64>`
    pub fn as_array_i64_mut(&mut self) -> Result<&mut Array<i64>> {
        match self {
            TypedValue::ArrayI64(a) => Ok(a),
            _ => Err(AzothError::InvalidState("Not an Array<i64>".into())),
        }
    }

    /// Extract as `Array<u64>`
    pub fn as_array_u64(&self) -> Result<&Array<u64>> {
        match self {
            TypedValue::ArrayU64(a) => Ok(a),
            _ => Err(AzothError::InvalidState("Not an Array<u64>".into())),
        }
    }

    /// Extract as mutable `Array<u64>`
    pub fn as_array_u64_mut(&mut self) -> Result<&mut Array<u64>> {
        match self {
            TypedValue::ArrayU64(a) => Ok(a),
            _ => Err(AzothError::InvalidState("Not an Array<u64>".into())),
        }
    }

    /// Extract as `Array<u128>`
    pub fn as_array_u128(&self) -> Result<&Array<u128>> {
        match self {
            TypedValue::ArrayU128(a) => Ok(a),
            _ => Err(AzothError::InvalidState("Not an Array<u128>".into())),
        }
    }

    /// Extract as mutable `Array<u128>`
    pub fn as_array_u128_mut(&mut self) -> Result<&mut Array<u128>> {
        match self {
            TypedValue::ArrayU128(a) => Ok(a),
            _ => Err(AzothError::InvalidState("Not an Array<u128>".into())),
        }
    }

    /// Extract as `Set<Vec<u8>>`
    pub fn as_set_bytes(&self) -> Result<&Set<Vec<u8>>> {
        match self {
            TypedValue::SetBytes(s) => Ok(s),
            _ => Err(AzothError::InvalidState("Not a Set<Vec<u8>>".into())),
        }
    }

    /// Extract as mutable `Set<Vec<u8>>`
    pub fn as_set_bytes_mut(&mut self) -> Result<&mut Set<Vec<u8>>> {
        match self {
            TypedValue::SetBytes(s) => Ok(s),
            _ => Err(AzothError::InvalidState("Not a Set<Vec<u8>>".into())),
        }
    }

    /// Extract as `Array<Vec<u8>>`
    pub fn as_array_bytes(&self) -> Result<&Array<Vec<u8>>> {
        match self {
            TypedValue::ArrayBytes(a) => Ok(a),
            _ => Err(AzothError::InvalidState("Not an Array<Vec<u8>>".into())),
        }
    }

    /// Extract as mutable `Array<Vec<u8>>`
    pub fn as_array_bytes_mut(&mut self) -> Result<&mut Array<Vec<u8>>> {
        match self {
            TypedValue::ArrayBytes(a) => Ok(a),
            _ => Err(AzothError::InvalidState("Not an Array<Vec<u8>>".into())),
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
    fn test_u256_checked_add() {
        // Basic addition
        let a = U256::from_u64(100);
        let b = U256::from_u64(200);
        let result = a.checked_add(&b).unwrap();
        assert_eq!(result, U256::from_u64(300));

        // Addition with carry propagation
        let a = U256([u64::MAX, 0, 0, 0]);
        let b = U256([1, 0, 0, 0]);
        let result = a.checked_add(&b).unwrap();
        assert_eq!(result, U256([0, 1, 0, 0]));

        // Overflow detection
        let a = U256([u64::MAX, u64::MAX, u64::MAX, u64::MAX]);
        let b = U256([1, 0, 0, 0]);
        assert!(a.checked_add(&b).is_none());
    }

    #[test]
    fn test_u256_checked_sub() {
        // Basic subtraction
        let a = U256::from_u64(300);
        let b = U256::from_u64(100);
        let result = a.checked_sub(&b).unwrap();
        assert_eq!(result, U256::from_u64(200));

        // Subtraction with borrow propagation
        let a = U256([0, 1, 0, 0]);
        let b = U256([1, 0, 0, 0]);
        let result = a.checked_sub(&b).unwrap();
        assert_eq!(result, U256([u64::MAX, 0, 0, 0]));

        // Underflow detection
        let a = U256::from_u64(100);
        let b = U256::from_u64(200);
        assert!(a.checked_sub(&b).is_none());

        // Zero result
        let a = U256::from_u64(100);
        let b = U256::from_u64(100);
        let result = a.checked_sub(&b).unwrap();
        assert_eq!(result, U256::zero());
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

    #[test]
    fn test_typed_set_u64() {
        let mut set = Set::new();
        set.insert(100u64);
        set.insert(200u64);
        set.insert(300u64);

        let value = TypedValue::SetU64(set);
        let bytes = value.to_bytes().unwrap();
        let decoded = TypedValue::from_bytes(&bytes).unwrap();

        let decoded_set = decoded.as_set_u64().unwrap();
        assert_eq!(decoded_set.len(), 3);
        assert!(decoded_set.contains(&100u64));
        assert!(decoded_set.contains(&200u64));
        assert!(!decoded_set.contains(&400u64));
    }

    #[test]
    fn test_typed_array_i64() {
        let mut array = Array::new();
        array.push(-100i64);
        array.push(0i64);
        array.push(100i64);

        let value = TypedValue::ArrayI64(array);
        let bytes = value.to_bytes().unwrap();
        let decoded = TypedValue::from_bytes(&bytes).unwrap();

        let decoded_array = decoded.as_array_i64().unwrap();
        assert_eq!(decoded_array.len(), 3);
        assert_eq!(decoded_array.get(0), Some(&-100i64));
        assert_eq!(decoded_array.get(1), Some(&0i64));
        assert_eq!(decoded_array.get(2), Some(&100i64));
    }

    #[test]
    fn test_typed_set_bytes() {
        let mut set = Set::new();
        set.insert(vec![1, 2, 3]);
        set.insert(vec![4, 5, 6]);

        let value = TypedValue::SetBytes(set);
        let bytes = value.to_bytes().unwrap();
        let decoded = TypedValue::from_bytes(&bytes).unwrap();

        let decoded_set = decoded.as_set_bytes().unwrap();
        assert_eq!(decoded_set.len(), 2);
        assert!(decoded_set.contains(&vec![1, 2, 3]));
        assert!(decoded_set.contains(&vec![4, 5, 6]));
    }

    #[test]
    fn test_typed_array_u128() {
        let mut array = Array::new();
        array.push(u128::MAX);
        array.push(u128::MIN);
        array.push(12345u128);

        let value = TypedValue::ArrayU128(array);
        let bytes = value.to_bytes().unwrap();
        let decoded = TypedValue::from_bytes(&bytes).unwrap();

        let decoded_array = decoded.as_array_u128().unwrap();
        assert_eq!(decoded_array.len(), 3);
        assert_eq!(decoded_array.get(0), Some(&u128::MAX));
        assert_eq!(decoded_array.get(1), Some(&u128::MIN));
        assert_eq!(decoded_array.get(2), Some(&12345u128));
    }

    #[test]
    fn test_typed_collection_type_safety() {
        let set_u64 = TypedValue::SetU64(Set::new());

        // Should fail when trying to extract as wrong type
        assert!(set_u64.as_set().is_err());
        assert!(set_u64.as_set_i64().is_err());
        assert!(set_u64.as_array_u64().is_err());

        // Should succeed with correct type
        assert!(set_u64.as_set_u64().is_ok());
    }

    #[test]
    fn test_json_serialization() {
        use serde::{Deserialize, Serialize};

        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct User {
            id: String,
            name: String,
            age: u32,
        }

        let user = User {
            id: "user123".to_string(),
            name: "Alice".to_string(),
            age: 30,
        };

        // Serialize to TypedValue
        let value = TypedValue::from_json(&user).unwrap();

        // Should be Bytes variant
        match &value {
            TypedValue::Bytes(_) => {}
            _ => panic!("Expected Bytes variant"),
        }

        // Deserialize back
        let decoded: User = value.to_json().unwrap();
        assert_eq!(decoded, user);
    }

    #[test]
    fn test_json_roundtrip_complex() {
        use serde::{Deserialize, Serialize};
        use std::collections::HashMap;

        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct Instance {
            id: String,
            state: HashMap<String, String>,
            version: u64,
            metadata: Option<String>,
        }

        let mut state = HashMap::new();
        state.insert("key1".to_string(), "value1".to_string());
        state.insert("key2".to_string(), "value2".to_string());

        let instance = Instance {
            id: "instance123".to_string(),
            state,
            version: 42,
            metadata: Some("test metadata".to_string()),
        };

        // Round-trip
        let value = TypedValue::from_json(&instance).unwrap();
        let decoded: Instance = value.to_json().unwrap();

        assert_eq!(decoded.id, instance.id);
        assert_eq!(decoded.state, instance.state);
        assert_eq!(decoded.version, instance.version);
        assert_eq!(decoded.metadata, instance.metadata);
    }

    #[test]
    fn test_json_type_safety() {
        use serde::{Deserialize, Serialize};

        #[derive(Serialize, Deserialize)]
        struct User {
            id: String,
        }

        #[derive(Deserialize)]
        struct Post {
            id: String,
            title: String,
        }

        let user = User {
            id: "user123".to_string(),
        };

        let value = TypedValue::from_json(&user).unwrap();

        // Should fail when deserializing to wrong type
        let result: Result<Post> = value.to_json();
        assert!(result.is_err());
    }

    #[test]
    fn test_json_wrong_variant() {
        // Create a non-Bytes variant
        let value = TypedValue::I64(42);

        // Should fail when trying to deserialize from non-Bytes
        let result: Result<String> = value.to_json();
        assert!(result.is_err());
    }
}
