use crate::args::Args;
use anyhow::{Context, Result};
use tokio_postgres::{Client, NoTls};
use tracing::{debug, warn};

pub struct DbClient {
    pub client: Client,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub dbname: String,
}

impl DbClient {
    /// Connect using the provided args.
    pub async fn connect(args: &Args) -> Result<Self> {
        Self::connect_to(
            &args.host,
            args.port,
            &args.user,
            &args.dbname,
            args.password.as_deref(),
        )
        .await
    }

    /// Connect to an explicit target (used by \c reconnect).
    pub async fn connect_to(
        host: &str,
        port: u16,
        user: &str,
        dbname: &str,
        password: Option<&str>,
    ) -> Result<Self> {
        let pwd = password.unwrap_or("");
        let conn_str = format!(
            "host={} port={} user={} dbname={} password={}",
            host, port, user, dbname, pwd
        );
        debug!(
            "Connecting: host={} port={} user={} dbname={}",
            host, port, user, dbname
        );

        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .with_context(|| format!("Failed to connect to {}:{}/{}", host, port, dbname))?;

        // Spawn the connection driver
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                warn!("Connection error: {}", e);
            }
        });

        Ok(Self {
            client,
            host: host.to_string(),
            port,
            user: user.to_string(),
            dbname: dbname.to_string(),
        })
    }

    /// Execute a query and return SimpleQueryRow results (for SELECT).
    pub async fn query_simple(
        &self,
        sql: &str,
    ) -> Result<(Vec<tokio_postgres::SimpleQueryRow>, String)> {
        let messages = self
            .client
            .simple_query(sql)
            .await
            .with_context(|| format!("Failed to execute: {}", sql))?;

        let mut rows = Vec::new();
        let mut tag = String::new();

        for msg in messages {
            match msg {
                tokio_postgres::SimpleQueryMessage::Row(r) => rows.push(r),
                tokio_postgres::SimpleQueryMessage::CommandComplete(n) => {
                    tag = format!("{} rows", n);
                }
                _ => {}
            }
        }

        Ok((rows, tag))
    }

    pub fn conninfo(&self) -> String {
        format!(
            "You are connected to database \"{}\" as user \"{}\" on host \"{}\" port {}.",
            self.dbname, self.user, self.host, self.port
        )
    }
}
