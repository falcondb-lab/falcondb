use anyhow::{Context, Result};
use clap::Parser;
use falcon_cli::{client::DbClient, csv::CsvOptions, import::{ImportCmd, run_import}};

/// FalconDB high-performance import tool.
///
/// Streams a CSV file into FalconDB using COPY FROM STDIN.
///
/// Examples:
///   fimport -h 127.0.0.1 -p 5433 -U falcon -d mydb \
///     --file orders.csv --table orders --header
///
///   fimport ... --file data.tsv --table orders --delimiter $'\t' --no-copy --batch-size 5000
#[derive(Parser, Debug)]
#[command(name = "fimport", version, about = "Import CSV into a FalconDB table")]
struct Cli {
    #[arg(short = 'H', long, default_value = "127.0.0.1", env = "FALCON_HOST")]
    host: String,

    #[arg(short, long, default_value_t = 5433, env = "FALCON_PORT")]
    port: u16,

    #[arg(short = 'U', long, default_value = "falcon", env = "FALCON_USER")]
    user: String,

    #[arg(short, long, default_value = "falcon", env = "FALCON_DB")]
    dbname: String,

    #[arg(short = 'W', long, env = "FALCON_PASSWORD")]
    password: Option<String>,

    /// Input CSV file path.
    #[arg(short, long)]
    file: String,

    /// Target table name.
    #[arg(short, long)]
    table: String,

    /// CSV file has a header row.
    #[arg(long)]
    header: bool,

    /// Field delimiter (default: comma). Use \t for tab.
    #[arg(long, default_value = ",")]
    delimiter: String,

    /// String that represents NULL in the CSV.
    #[arg(long, default_value = "")]
    null_as: String,

    /// Quote character (default: double-quote).
    #[arg(long, default_value = "\"")]
    quote: String,

    /// Rows per INSERT batch (only used with --no-copy).
    #[arg(long, default_value_t = 1000)]
    batch_size: usize,

    /// Stop on first error.
    #[arg(long)]
    on_error_stop: bool,

    /// Use multi-value INSERT instead of COPY FROM STDIN.
    #[arg(long)]
    no_copy: bool,
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("ERROR: {e:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();

    let delim = parse_single_char(&cli.delimiter, "delimiter")?;
    let quote = parse_single_char(&cli.quote, "quote")?;

    let opts = CsvOptions {
        header: cli.header,
        delimiter: delim as u8,
        null_as: cli.null_as.clone(),
        quote: quote as u8,
        escape: quote as u8,
    };

    let cmd = ImportCmd {
        file: cli.file.clone(),
        table: cli.table.clone(),
        opts,
        batch_size: cli.batch_size,
        on_error_stop: cli.on_error_stop,
        use_copy: !cli.no_copy,
    };

    let client = DbClient::connect_to(
        &cli.host,
        cli.port,
        &cli.user,
        &cli.dbname,
        cli.password.as_deref(),
    )
    .await
    .context("Connection failed")?;

    let s = run_import(&client, &cmd).await?;
    println!(
        "Import complete: {} total, {} inserted, {} failed.",
        s.total, s.inserted, s.failed
    );
    Ok(())
}

fn parse_single_char(s: &str, name: &str) -> Result<char> {
    let s = match s {
        "\\t" => "\t",
        "\\n" => "\n",
        other => other,
    };
    let mut chars = s.chars();
    let c = chars.next().ok_or_else(|| anyhow::anyhow!("{name} must not be empty"))?;
    if chars.next().is_some() {
        anyhow::bail!("{name} must be a single character, got {:?}", s);
    }
    Ok(c)
}
