//! Schedule types and utilities.

use crate::error::{Result, SchedulerError};
use chrono::{DateTime, Duration, Utc};
use cron::Schedule as CronSchedule;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// Schedule configuration for a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Schedule {
    /// Cron expression (e.g., "0 0 * * *").
    Cron {
        /// Cron expression string.
        expression: String,
    },
    /// Interval in seconds.
    Interval {
        /// Number of seconds between executions.
        seconds: u64,
    },
    /// One-time execution at a specific time.
    OneTime {
        /// Unix timestamp when to run.
        run_at: i64,
    },
    /// Execute immediately (as soon as possible).
    Immediate,
}

impl Schedule {
    /// Calculate the next run time after the given time.
    pub fn next_run_time(&self, after: DateTime<Utc>) -> Result<Option<DateTime<Utc>>> {
        match self {
            Self::Cron { expression } => {
                let schedule = CronSchedule::from_str(expression)?;
                Ok(schedule.after(&after).next())
            }
            Self::Interval { seconds } => {
                let next = after + Duration::seconds(*seconds as i64);
                Ok(Some(next))
            }
            Self::OneTime { run_at } => {
                let run_time = DateTime::from_timestamp(*run_at, 0)
                    .ok_or_else(|| SchedulerError::InvalidSchedule("Invalid timestamp".into()))?;
                if run_time > after {
                    Ok(Some(run_time))
                } else {
                    Ok(None)
                }
            }
            Self::Immediate => Ok(Some(after)),
        }
    }

    /// Validate the schedule configuration.
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Cron { expression } => {
                CronSchedule::from_str(expression)?;
                Ok(())
            }
            Self::Interval { seconds } => {
                if *seconds == 0 {
                    return Err(SchedulerError::InvalidSchedule(
                        "Interval must be greater than 0".into(),
                    ));
                }
                Ok(())
            }
            Self::OneTime { run_at } => {
                if DateTime::from_timestamp(*run_at, 0).is_none() {
                    return Err(SchedulerError::InvalidSchedule("Invalid timestamp".into()));
                }
                Ok(())
            }
            Self::Immediate => Ok(()),
        }
    }

    /// Check if this schedule is recurring.
    pub fn is_recurring(&self) -> bool {
        matches!(self, Self::Cron { .. } | Self::Interval { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cron_next_run_time() {
        let schedule = Schedule::Cron {
            expression: "0 0 0 * * *".to_string(), // Daily at midnight (6-field with seconds)
        };

        let now = Utc::now();
        let next = schedule.next_run_time(now).unwrap().unwrap();
        assert!(next > now);
    }

    #[test]
    fn test_interval_next_run_time() {
        let schedule = Schedule::Interval { seconds: 300 }; // 5 minutes

        let now = Utc::now();
        let next = schedule.next_run_time(now).unwrap().unwrap();
        assert_eq!((next - now).num_seconds(), 300);
    }

    #[test]
    fn test_one_time_next_run_time() {
        let future = Utc::now() + Duration::hours(1);
        let schedule = Schedule::OneTime {
            run_at: future.timestamp(),
        };

        let now = Utc::now();
        let next = schedule.next_run_time(now).unwrap().unwrap();
        assert_eq!(next.timestamp(), future.timestamp());
    }

    #[test]
    fn test_immediate_next_run_time() {
        let schedule = Schedule::Immediate;
        let now = Utc::now();
        let next = schedule.next_run_time(now).unwrap().unwrap();
        assert_eq!(next, now);
    }

    #[test]
    fn test_validate_cron() {
        let valid = Schedule::Cron {
            expression: "0 0 0 * * *".to_string(), // 6-field: sec min hour day month dow
        };
        assert!(valid.validate().is_ok());

        let invalid = Schedule::Cron {
            expression: "invalid".to_string(),
        };
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn test_validate_interval() {
        let valid = Schedule::Interval { seconds: 300 };
        assert!(valid.validate().is_ok());

        let invalid = Schedule::Interval { seconds: 0 };
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn test_is_recurring() {
        assert!(Schedule::Cron {
            expression: "0 0 0 * * *".to_string()
        }
        .is_recurring());
        assert!(Schedule::Interval { seconds: 300 }.is_recurring());
        assert!(!Schedule::OneTime { run_at: 0 }.is_recurring());
        assert!(!Schedule::Immediate.is_recurring());
    }
}
