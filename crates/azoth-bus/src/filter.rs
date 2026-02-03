/// Simple event representation for filtering
#[derive(Debug, Clone)]
pub struct Event {
    pub id: u64,
    pub event_type: String,
    pub payload: Vec<u8>,
}

impl Event {
    /// Decode from raw event bytes (format: "type:json_payload")
    ///
    /// The format from ctx.log is "event_type:json_payload" where json_payload
    /// is the JSON-serialized form of the payload. Since event_type may contain
    /// colons (e.g., "knowledge:fact_learned"), we need to find the LAST colon
    /// that's followed by valid JSON by actually attempting to parse.
    pub fn decode(id: u64, bytes: &[u8]) -> crate::Result<Self> {
        let s = std::str::from_utf8(bytes)
            .map_err(|e| crate::BusError::InvalidState(format!("Invalid UTF-8 in event: {}", e)))?;

        // Find all colons and check which one precedes valid JSON
        let colon_positions: Vec<usize> = s
            .char_indices()
            .filter(|(_, c)| *c == ':')
            .map(|(i, _)| i)
            .collect();

        // Check colons from last to first to find the split point
        for &split_pos in colon_positions.iter().rev() {
            let payload = &s[split_pos + 1..];

            // Try to parse as JSON to verify this is the correct split point
            // We don't care about the actual parsed value, just that it's valid JSON
            if serde_json::from_str::<serde_json::Value>(payload).is_ok() {
                let event_type = &s[..split_pos];
                return Ok(Self {
                    id,
                    event_type: event_type.to_string(),
                    payload: payload.as_bytes().to_vec(),
                });
            }
        }

        // No colon found or no valid JSON after any colon - legacy format
        Ok(Self {
            id,
            event_type: s.to_string(),
            payload: Vec::new(),
        })
    }
}

/// Filter for events
pub trait EventFilterTrait: Send + Sync {
    /// Check if event matches filter
    fn matches(&self, event: &Event) -> bool;
}

/// Event filter implementation
#[derive(Clone)]
pub enum EventFilter {
    /// Match all events
    All,

    /// Match events with type prefix
    Prefix(String),

    /// Match exact event type
    Exact(String),

    /// Combine multiple filters with AND logic
    And(Box<EventFilter>, Box<EventFilter>),

    /// Combine multiple filters with OR logic
    Or(Box<EventFilter>, Box<EventFilter>),
}

impl EventFilter {
    /// Create a prefix filter
    pub fn prefix(prefix: impl Into<String>) -> Self {
        EventFilter::Prefix(prefix.into())
    }

    /// Create an exact match filter
    pub fn exact(event_type: impl Into<String>) -> Self {
        EventFilter::Exact(event_type.into())
    }

    /// Combine with another filter using AND logic
    pub fn and(self, other: EventFilter) -> Self {
        EventFilter::And(Box::new(self), Box::new(other))
    }

    /// Combine with another filter using OR logic
    pub fn or(self, other: EventFilter) -> Self {
        EventFilter::Or(Box::new(self), Box::new(other))
    }
}

impl EventFilterTrait for EventFilter {
    fn matches(&self, event: &Event) -> bool {
        match self {
            EventFilter::All => true,
            EventFilter::Prefix(prefix) => event.event_type.starts_with(prefix),
            EventFilter::Exact(event_type) => &event.event_type == event_type,
            EventFilter::And(f1, f2) => f1.matches(event) && f2.matches(event),
            EventFilter::Or(f1, f2) => f1.matches(event) || f2.matches(event),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_event(event_type: &str) -> Event {
        Event {
            id: 1,
            event_type: event_type.to_string(),
            payload: vec![],
        }
    }

    #[test]
    fn test_prefix_filter() {
        let filter = EventFilter::prefix("knowledge:");
        assert!(filter.matches(&test_event("knowledge:doc_updated")));
        assert!(!filter.matches(&test_event("bus:message")));
    }

    #[test]
    fn test_exact_filter() {
        let filter = EventFilter::exact("knowledge:doc_updated");
        assert!(filter.matches(&test_event("knowledge:doc_updated")));
        assert!(!filter.matches(&test_event("knowledge:doc_deleted")));
    }

    #[test]
    fn test_and_filter() {
        let filter = EventFilter::prefix("knowledge:").and(EventFilter::prefix("knowledge:doc_"));

        assert!(filter.matches(&test_event("knowledge:doc_updated")));
        assert!(!filter.matches(&test_event("knowledge:index_updated")));
    }

    #[test]
    fn test_or_filter() {
        let filter = EventFilter::prefix("knowledge:").or(EventFilter::prefix("bus:"));

        assert!(filter.matches(&test_event("knowledge:doc_updated")));
        assert!(filter.matches(&test_event("bus:message")));
        assert!(!filter.matches(&test_event("session:started")));
    }
}
