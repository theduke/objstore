use std::time::Duration;

use dioxus::prelude::*;
use objstore::ObjectMeta;
use time::OffsetDateTime;

pub fn object_size(item: &ObjectMeta) -> String {
    if let Some(size) = item.size {
        human_size(size)
    } else {
        String::new()
    }
}

pub fn object_created(item: &ObjectMeta, now: time::OffsetDateTime) -> Element {
    if let Some(created) = &item.created_at {
        human_diff_date(*created, now)
    } else {
        rsx! {
            span {
                "n/a"
            }
        }
    }
}

pub fn object_modified(item: &ObjectMeta, now: time::OffsetDateTime) -> Element {
    if let Some(time) = &item.updated_at {
        human_diff_date(*time, now)
    } else {
        rsx! {
            span {
                "n/a"
            }
        }
    }
}

pub fn human_size(size: u64) -> String {
    if size < 1024 {
        format!("{}b", size)
    } else if size < 1024 * 1024 {
        format!("{:.1}KB", size as f64 / 1024.0)
    } else if size < 1024 * 1024 * 1024 {
        format!("{:.1}MB", size as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.1}GB", size as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

pub fn human_diff_date(date: time::OffsetDateTime, now: OffsetDateTime) -> Element {
    let diff = match (now - date).try_into() {
        Ok(duration) => human_duration(duration),
        Err(_) => "n/a".to_string(),
    };

    let title = date
        .format(&time::format_description::well_known::Iso8601::DEFAULT)
        .unwrap_or_default();

    rsx! {
        span {
            title,
            "{diff}"
        }
    }
}

pub fn human_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    let mins = secs / 60;
    let hours = mins / 60;
    let days = hours / 24;

    if days > 0 {
        format!("{}d", days)
    } else if hours > 0 {
        format!("{}h", hours)
    } else if mins > 0 {
        format!("{}m", mins)
    } else {
        format!("{}s", secs)
    }
}
