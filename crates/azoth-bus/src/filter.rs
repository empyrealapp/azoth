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
    /// is the JSON-serialized form of the payload. Since JSON always starts with
    /// a specific character (" for strings, { for objects, [ for arrays, etc.),
    /// we can find the last ':' before valid JSON starts.
    pub fn decode(id: u64, bytes: &[u8]) -> crate::Result<Self> {
        let s = std::str::from_utf8(bytes)
            .map_err(|e| crate::BusError::InvalidState(format!("Invalid UTF-8 in event: {}", e)))?;

        // Find the last ':' followed by JSON (starts with ", {, [, true, false, null, or digit)
        if let Some(split_pos) = s.rfind(':') {
            let event_type = &s[..split_pos];
            let payload = &s[split_pos + 1..];

            // Verify payload looks like JSON
            if payload.starts_with('"')
                || payload.starts_with('{')
                || payload.starts_with('[')
                || payload.starts_with("true")
                || payload.starts_with("false")
                || payload.starts_with("null")
                || payload
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_digit() || c == '-')
            {
                return Ok(Self {
                    id,
                    event_type: event_type.to_string(),
                    payload: payload.as_bytes().to_vec(),
                });
            }
        }

        // Legacy format or malformed: entire content is the type
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
