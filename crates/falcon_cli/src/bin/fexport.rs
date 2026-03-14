use anyhow::{Context, Result};
use clap::Parser;
use falcon_cli::{
    client::DbClient,
    csv::CsvOptions,
    export::{run_export, ExportCmd},
};

/// FalconDB high-performance export tool.
///
/// Streams data from FalconDB to a CSV file using COPY TO STDOUT.
///
/// Examples:
///   fexport -h 127.0.0.1 -p 5433 -U falcon -d mydb \
///     --query "SELECT * FROM orders" --file orders.csv --header
///
///   fexport ... --query orders --file orders.csv --delimiter '|'
#[derive(Parser, Debug)]
#[command(
    name = "fexport",
    version,
    about = "Export FalconDB table/query to CSV"
)]
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

    /// SQL query or bare table name to export.
    #[arg(short, long)]
    query: String,

    /// Output file path.
    #[arg(short, long)]
    file: String,

    /// Write CSV header row.
    #[arg(long)]
    header: bool,

    /// Field delimiter (default: comma).
    #[arg(long, default_value = ",")]
    delimiter: String,

    /// String to use for NULL values.
    #[arg(long, default_value = "")]
    null_as: String,

    /// Quote character (default: double-quote).
    #[arg(long, default_value = "\"")]
    quote: String,

    /// Overwrite output file if it exists.
    #[arg(long)]
    overwrite: bool,

    /// Use SELECT instead of COPY (slower, for debugging).
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

    let cmd = ExportCmd {
        query: cli.query.clone(),
        file: cli.file.clone(),
        opts,
        overwrite: cli.overwrite,
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

    let n = run_export(&client, &cmd).await?;
    println!("Exported {} rows to '{}'.", n, cli.file);
    Ok(())
}

fn parse_single_char(s: &str, name: &str) -> Result<char> {
    let s = match s {
        "\\t" => "\t",
        "\\n" => "\n",
        other => other,
    };
    let mut chars = s.chars();
    let c = chars
        .next()
        .ok_or_else(|| anyhow::anyhow!("{name} must not be empty"))?;
    if chars.next().is_some() {
        anyhow::bail!("{name} must be a single character, got {:?}", s);
    }
    Ok(c)
}
