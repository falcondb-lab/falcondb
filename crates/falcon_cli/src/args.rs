use clap::Parser;

/// fsql — FalconDB interactive CLI client
#[derive(Debug, Parser)]
#[command(
    name = "fsql",
    about = "FalconDB interactive SQL client (psql-compatible)",
    version
)]
pub struct Args {
    /// Database host
    #[arg(short = 'h', long, env = "PGHOST", default_value = "localhost")]
    pub host: String,

    /// Database port
    #[arg(short = 'p', long, env = "PGPORT", default_value_t = 5432)]
    pub port: u16,

    /// Database user
    #[arg(short = 'U', long, env = "PGUSER", default_value = "falcon")]
    pub user: String,

    /// Database name
    #[arg(short = 'd', long, env = "PGDATABASE", default_value = "falcon")]
    pub dbname: String,

    /// Password (use PGPASSWORD env var to avoid prompt)
    #[arg(short = 'W', long, env = "PGPASSWORD")]
    pub password: Option<String>,

    /// SSL mode (accepted but may be placeholder in v0.1)
    #[arg(long, default_value = "prefer")]
    pub sslmode: SslMode,

    /// Execute a single SQL command and exit
    #[arg(short = 'c', long)]
    pub command: Option<String>,

    /// Execute SQL from a file and exit
    #[arg(short = 'f', long)]
    pub file: Option<String>,

    /// Tuples only — suppress headers and formatting
    #[arg(short = 't', long)]
    pub tuples_only: bool,

    /// Expanded (vertical) output mode
    #[arg(long)]
    pub expanded: bool,

    /// CSV output mode
    #[arg(long)]
    pub csv: bool,

    /// JSON output mode
    #[arg(long)]
    pub json: bool,

    /// Set a variable (e.g. -v ON_ERROR_STOP=1)
    #[arg(short = 'v', long = "variable", value_name = "NAME=VALUE")]
    pub variables: Vec<String>,

    /// Apply a management command (skip plan-only mode)
    #[arg(long)]
    pub apply: bool,

    /// Skip interactive confirmation prompt (use with --apply)
    #[arg(long)]
    pub yes: bool,

    /// Output mode: interactive (default) or machine (JSON-only, no pager)
    #[arg(long, default_value = "interactive")]
    pub mode: String,

    /// Operator identity for audit attribution (overrides FSQL_OPERATOR env var)
    #[arg(long, env = "FSQL_OPERATOR")]
    pub operator: Option<String>,

    /// Source attribution: human or automation (overrides FSQL_SOURCE env var)
    #[arg(long, env = "FSQL_SOURCE")]
    pub source: Option<String>,
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum SslMode {
    Disable,
    Prefer,
    Require,
}

impl Args {
    /// Parse -v NAME=VALUE pairs into a key-value map.
    pub fn on_error_stop(&self) -> bool {
        self.variables.iter().any(|v| {
            v.eq_ignore_ascii_case("ON_ERROR_STOP=1")
                || v.eq_ignore_ascii_case("ON_ERROR_STOP=true")
        })
    }
}
