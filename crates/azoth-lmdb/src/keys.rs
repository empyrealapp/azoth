use azoth_core::EventId;

/// Format EventId as big-endian bytes (preserves sort order in LMDB)
pub fn event_id_to_key(id: EventId) -> [u8; 8] {
    id.to_be_bytes()
}

/// Parse EventId from big-endian bytes
pub fn key_to_event_id(bytes: &[u8]) -> Option<EventId> {
    if bytes.len() == 8 {
        Some(EventId::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    } else {
        None
    }
}

/// Meta keys used in the meta database
pub mod meta_keys {
    pub const NEXT_EVENT_ID: &str = "next_event_id";
    pub const SEALED_EVENT_ID: &str = "sealed_event_id";
    pub const SCHEMA_VERSION: &str = "schema_version";
    pub const CREATED_AT: &str = "created_at";
    pub const UPDATED_AT: &str = "updated_at";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_id_encoding() {
        let id: EventId = 12345;
        let key = event_id_to_key(id);
        let decoded = key_to_event_id(&key).unwrap();
        assert_eq!(id, decoded);
    }

    #[test]
    fn test_sort_order() {
        // Big-endian ensures lexicographic sort = numeric sort
        let id1 = event_id_to_key(100);
        let id2 = event_id_to_key(200);
        assert!(id1 < id2);
    }
}
