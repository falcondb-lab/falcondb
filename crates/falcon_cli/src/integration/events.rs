use crate::client::DbClient;
use crate::format::OutputMode;
use crate::runner::format_rows_as_string;
use anyhow::Result;

/// Sub-command for \events.
#[derive(Debug, Clone, PartialEq)]
pub enum EventsCmd {
    List,
    Tail,
    Replay(String),
}

impl EventsCmd {
    pub fn parse(arg: &str) -> Self {
        let s = arg.trim();
        let lower = s.to_lowercase();
        let mut parts = s.splitn(2, char::is_whitespace);
        let verb = parts.next().unwrap_or("").to_lowercase();
        let rest = parts.next().unwrap_or("").trim().to_string();

        match verb.as_str() {
            "tail" => Self::Tail,
            "replay" => Self::Replay(rest),
            _ => {
                let _ = lower;
                Self::List
            }
        }
    }
}

pub async fn run_events(client: &DbClient, cmd: &EventsCmd, mode: OutputMode) -> Result<String> {
    match cmd {
        EventsCmd::List => events_list(client, mode).await,
        EventsCmd::Tail => events_tail(client, mode).await,
        EventsCmd::Replay(id) => events_replay(client, id, mode).await,
    }
}

async fn events_list(client: &DbClient, mode: OutputMode) -> Result<String> {
    let sql = "SELECT event_id, event_time, event_type, severity, source, \
                      operator, payload \
               FROM falcon.events \
               ORDER BY event_time DESC \
               LIMIT 100";
    let (rows, _) = client
        .query_simple(sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        // Fallback: derive from audit log
        let fallback = "SELECT event_id, event_time, 'audit' AS event_type, \
                               'info' AS severity, 'fsql' AS source, \
                               operator, command_issued AS payload \
                        FROM falcon.audit_log \
                        ORDER BY event_time DESC \
                        LIMIT 100";
        let (fb_rows, _) = client
            .query_simple(fallback)
            .await
            .unwrap_or_else(|_| (Vec::new(), String::new()));

        if fb_rows.is_empty() {
            return Ok(
                "No events found. (falcon.events view not present or no events recorded)\n"
                    .to_string(),
            );
        }
        return Ok(format_rows_as_string(&fb_rows, mode, "Events"));
    }

    Ok(format_rows_as_string(&rows, mode, "Events"))
}

async fn events_tail(client: &DbClient, mode: OutputMode) -> Result<String> {
    // Tail: last 20 events, most recent first
    let sql = "SELECT event_id, event_time, event_type, severity, source, \
                      operator, payload \
               FROM falcon.events \
               ORDER BY event_time DESC \
               LIMIT 20";
    let (rows, _) = client
        .query_simple(sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        return Ok("No recent events. (falcon.events view not present or empty)\n".to_string());
    }

    Ok(format_rows_as_string(&rows, mode, "Recent Events (tail)"))
}

async fn events_replay(client: &DbClient, event_id: &str, mode: OutputMode) -> Result<String> {
    if event_id.is_empty() {
        return Ok("Usage: \\events replay <event_id>\n".to_string());
    }

    // Fetch the event
    let sql = format!(
        "SELECT event_id, event_time, event_type, severity, source, \
                operator, payload \
         FROM falcon.events \
         WHERE event_id = '{}'",
        event_id.replace('\'', "''")
    );
    let (rows, _) = client
        .query_simple(&sql)
        .await
        .unwrap_or_else(|_| (Vec::new(), String::new()));

    if rows.is_empty() {
        return Ok(format!("Event '{}' not found.\n", event_id));
    }

    // Record the replay in audit log
    let replay_sql = format!(
        "SELECT falcon.admin_replay_event('{}')",
        event_id.replace('\'', "''")
    );
    let replay_result = client.query_simple(&replay_sql).await;

    let event_out = format_rows_as_string(&rows, mode, &format!("Event {}", event_id));

    match replay_result {
        Ok(_) => Ok(format!(
            "{}\nEvent '{}' replayed and logged to audit trail.\n",
            event_out, event_id
        )),
        Err(_) => Ok(format!(
            "{}\nEvent '{}' fetched. \
             (falcon.admin_replay_event() not available — replay not logged)\n",
            event_out, event_id
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_events_cmd_parse_list() {
        assert_eq!(EventsCmd::parse("list"), EventsCmd::List);
        assert_eq!(EventsCmd::parse(""), EventsCmd::List);
        assert_eq!(EventsCmd::parse("LIST"), EventsCmd::List);
    }

    #[test]
    fn test_events_cmd_parse_tail() {
        assert_eq!(EventsCmd::parse("tail"), EventsCmd::Tail);
        assert_eq!(EventsCmd::parse("TAIL"), EventsCmd::Tail);
    }

    #[test]
    fn test_events_cmd_parse_replay() {
        assert_eq!(
            EventsCmd::parse("replay evt-42"),
            EventsCmd::Replay("evt-42".to_string())
        );
    }

    #[test]
    fn test_events_cmd_parse_replay_empty() {
        assert_eq!(
            EventsCmd::parse("replay"),
            EventsCmd::Replay("".to_string())
        );
    }
}
