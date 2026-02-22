//! NOTIFY/LISTEN — asynchronous notification channels.
//!
//! PostgreSQL allows sessions to subscribe to named channels via LISTEN and
//! receive notifications sent by other sessions via NOTIFY. This module
//! implements an in-memory broadcast hub that routes notifications to all
//! listeners on a given channel.
//!
//! Design:
//! - `NotificationHub` is a shared (Arc) object held by the server.
//! - Each session that executes LISTEN gets a `tokio::sync::broadcast::Receiver`.
//! - NOTIFY sends on the broadcast channel; all current listeners receive it.
//! - UNLISTEN removes the subscription.

use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use tokio::sync::broadcast;

/// A single notification payload.
#[derive(Debug, Clone)]
pub struct Notification {
    /// The channel name.
    pub channel: String,
    /// The sending session's backend process ID.
    pub sender_pid: i32,
    /// Optional payload string.
    pub payload: String,
}

/// Capacity of each channel's broadcast buffer.
const CHANNEL_CAPACITY: usize = 256;

/// Per-channel state: a broadcast sender.
struct ChannelState {
    tx: broadcast::Sender<Notification>,
}

/// Central hub for NOTIFY/LISTEN.
pub struct NotificationHub {
    channels: Mutex<HashMap<String, ChannelState>>,
}

impl NotificationHub {
    pub fn new() -> Self {
        Self {
            channels: Mutex::new(HashMap::new()),
        }
    }

    /// Send a notification on a channel. Returns the number of receivers that
    /// will see it (0 if nobody is listening).
    pub fn notify(&self, channel: &str, sender_pid: i32, payload: &str) -> usize {
        let channels = self.channels.lock();
        if let Some(state) = channels.get(channel) {
            let msg = Notification {
                channel: channel.to_string(),
                sender_pid,
                payload: payload.to_string(),
            };
            state.tx.send(msg).unwrap_or(0)
        } else {
            0
        }
    }

    /// Subscribe to a channel. Returns a Receiver that will yield future
    /// notifications. If the channel doesn't exist yet, it is created.
    pub fn listen(&self, channel: &str) -> broadcast::Receiver<Notification> {
        let mut channels = self.channels.lock();
        let state = channels.entry(channel.to_string()).or_insert_with(|| {
            let (tx, _) = broadcast::channel(CHANNEL_CAPACITY);
            ChannelState { tx }
        });
        state.tx.subscribe()
    }

    /// Remove a channel entirely if it has no more active senders.
    /// (This is best-effort cleanup; channels are cheap.)
    pub fn cleanup_channel(&self, channel: &str) {
        let mut channels = self.channels.lock();
        if let Some(state) = channels.get(channel) {
            if state.tx.receiver_count() == 0 {
                channels.remove(channel);
            }
        }
    }
}

impl Default for NotificationHub {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-session LISTEN state. Tracks which channels this session is subscribed
/// to and provides a method to drain pending notifications.
pub struct SessionNotifications {
    /// channel_name → broadcast::Receiver
    subscriptions: HashMap<String, broadcast::Receiver<Notification>>,
    /// Set of channel names we're listening on.
    channels: HashSet<String>,
}

impl SessionNotifications {
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
            channels: HashSet::new(),
        }
    }

    /// Subscribe to a channel via the hub.
    pub fn listen(&mut self, channel: &str, hub: &NotificationHub) {
        if self.channels.contains(channel) {
            return; // already listening
        }
        let rx = hub.listen(channel);
        self.subscriptions.insert(channel.to_string(), rx);
        self.channels.insert(channel.to_string());
    }

    /// Unsubscribe from a channel.
    pub fn unlisten(&mut self, channel: &str, hub: &NotificationHub) {
        self.subscriptions.remove(channel);
        self.channels.remove(channel);
        hub.cleanup_channel(channel);
    }

    /// Unsubscribe from all channels.
    pub fn unlisten_all(&mut self, hub: &NotificationHub) {
        let names: Vec<String> = self.channels.drain().collect();
        self.subscriptions.clear();
        for name in &names {
            hub.cleanup_channel(name);
        }
    }

    /// Drain all pending notifications across all subscribed channels.
    /// Returns them in the order received (best-effort).
    pub fn drain_pending(&mut self) -> Vec<Notification> {
        let mut pending = Vec::new();
        for rx in self.subscriptions.values_mut() {
            loop {
                match rx.try_recv() {
                    Ok(notif) => pending.push(notif),
                    Err(broadcast::error::TryRecvError::Empty) => break,
                    Err(broadcast::error::TryRecvError::Lagged(n)) => {
                        tracing::warn!("Notification receiver lagged by {} messages", n);
                        // Continue draining what's left
                    }
                    Err(broadcast::error::TryRecvError::Closed) => break,
                }
            }
        }
        pending
    }

    /// Whether this session has any active subscriptions.
    pub fn has_subscriptions(&self) -> bool {
        !self.channels.is_empty()
    }

    /// List of channels we're listening on.
    pub fn listening_channels(&self) -> Vec<&str> {
        self.channels.iter().map(|s| s.as_str()).collect()
    }
}

impl Default for SessionNotifications {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_notify_no_listeners() {
        let hub = NotificationHub::new();
        let count = hub.notify("test_channel", 1, "hello");
        assert_eq!(count, 0);
    }

    #[test]
    fn test_listen_and_notify() {
        let hub = NotificationHub::new();
        let mut session = SessionNotifications::new();
        session.listen("ch1", &hub);

        assert!(session.has_subscriptions());
        assert_eq!(session.listening_channels(), vec!["ch1"]);

        hub.notify("ch1", 42, "payload1");
        hub.notify("ch1", 42, "payload2");

        let pending = session.drain_pending();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].channel, "ch1");
        assert_eq!(pending[0].payload, "payload1");
        assert_eq!(pending[0].sender_pid, 42);
        assert_eq!(pending[1].payload, "payload2");
    }

    #[test]
    fn test_unlisten() {
        let hub = NotificationHub::new();
        let mut session = SessionNotifications::new();
        session.listen("ch1", &hub);
        session.unlisten("ch1", &hub);

        assert!(!session.has_subscriptions());

        hub.notify("ch1", 1, "after-unlisten");
        let pending = session.drain_pending();
        assert!(pending.is_empty());
    }

    #[test]
    fn test_unlisten_all() {
        let hub = NotificationHub::new();
        let mut session = SessionNotifications::new();
        session.listen("ch1", &hub);
        session.listen("ch2", &hub);
        assert_eq!(session.listening_channels().len(), 2);

        session.unlisten_all(&hub);
        assert!(!session.has_subscriptions());
    }

    #[test]
    fn test_multiple_sessions() {
        let hub = NotificationHub::new();
        let mut s1 = SessionNotifications::new();
        let mut s2 = SessionNotifications::new();

        s1.listen("events", &hub);
        s2.listen("events", &hub);

        let count = hub.notify("events", 10, "broadcast");
        assert_eq!(count, 2);

        assert_eq!(s1.drain_pending().len(), 1);
        assert_eq!(s2.drain_pending().len(), 1);
    }

    #[test]
    fn test_drain_empty() {
        let hub = NotificationHub::new();
        let mut session = SessionNotifications::new();
        session.listen("ch1", &hub);
        let pending = session.drain_pending();
        assert!(pending.is_empty());
    }
}
