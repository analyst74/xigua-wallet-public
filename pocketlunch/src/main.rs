use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde_json::json;
use tracing_subscriber::EnvFilter;

use pocketlunch::audit;
use pocketlunch::config::AppConfig;
use pocketlunch::lm_v2_client::LmV2Client;
use pocketlunch::models::parse_decisions;
use pocketlunch::service_api;
use pocketlunch::store::Store;
use pocketlunch::sync_engine::SyncEngine;

#[derive(Debug, Parser)]
#[command(name = "pocketlunch")]
#[command(about = "Lunch Money v2 local-first sync engine")]
struct Cli {
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    #[arg(
        long,
        env = "LUNCH_MONEY_BASE_URL",
        default_value = "https://api.lunchmoney.dev"
    )]
    base_url: String,

    #[arg(long, env = "LUNCH_MONEY_API_TOKEN")]
    api_token: Option<String>,

    #[arg(long, env = "LM_DEFAULT_LOOKBACK_SECONDS", default_value_t = 300)]
    default_lookback_seconds: i64,

    #[arg(long, env = "LM_PULL_PAGE_SIZE", default_value_t = 100)]
    pull_page_size: i64,

    #[arg(long, env = "LM_OUTBOX_BATCH_SIZE", default_value_t = 100)]
    outbox_batch_size: i64,

    #[arg(long, env = "LM_TRANSACTION_UPDATE_BATCH_SIZE", default_value_t = 50)]
    transaction_update_batch_size: i64,

    #[arg(long, env = "LM_MAX_PUSH_ATTEMPTS", default_value_t = 8)]
    max_push_attempts: i64,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Serve {
        #[arg(long, env = "POCKETLUNCH_SERVICE_BIND_HOST", default_value = "0.0.0.0")]
        host: String,
        #[arg(long, env = "POCKETLUNCH_SERVICE_BIND_PORT", default_value_t = 8080)]
        port: u16,
    },
    Sync {
        #[command(subcommand)]
        command: SyncSubcommand,
    },
    Local {
        #[command(subcommand)]
        command: LocalSubcommand,
    },
    Audit {
        #[command(subcommand)]
        command: AuditSubcommand,
    },
    Outbox {
        #[command(subcommand)]
        command: OutboxSubcommand,
    },
}

#[derive(Debug, Subcommand)]
enum SyncSubcommand {
    Pull,
    #[command(name = "pull-non-transactions")]
    PullNonTransactions,
    Push,
    All,
}

#[derive(Debug, Subcommand)]
enum LocalSubcommand {
    #[command(name = "categorize-apply")]
    CategorizeApply {
        #[arg(long)]
        file: String,
    },
    #[command(name = "category-delete")]
    CategoryDelete {
        #[arg(long)]
        category_id: i64,
        #[arg(long, default_value_t = false)]
        force: bool,
        #[arg(long)]
        reason: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum AuditSubcommand {
    List {
        #[arg(long)]
        run_id: Option<String>,
        #[arg(long)]
        entity: Option<String>,
        #[arg(long)]
        entity_id: Option<String>,
        #[arg(long, default_value_t = 200)]
        limit: i64,
    },
}

#[derive(Debug, Subcommand)]
enum OutboxSubcommand {
    Requeue {
        #[arg(long)]
        change_id: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let cli = Cli::parse();
    let config = AppConfig::new(
        cli.database_url.clone(),
        cli.base_url.clone(),
        cli.api_token.clone(),
        cli.default_lookback_seconds,
        cli.pull_page_size,
        cli.outbox_batch_size,
        cli.transaction_update_batch_size,
        cli.max_push_attempts,
    )?;
    let store = Store::new(&config).await?;
    store.bootstrap().await?;

    match cli.command {
        Commands::Serve { host, port } => {
            service_api::run_http_server(config, store, host, port).await?;
        }
        Commands::Sync { command } => {
            let token = config
                .api_token
                .clone()
                .context("LUNCH_MONEY_API_TOKEN is required for sync commands")?;

            let client = LmV2Client::new(config.base_url.clone(), token);
            let engine = SyncEngine::new(config.clone(), store.clone(), client);

            let run_id = match command {
                SyncSubcommand::Pull => engine.sync_pull().await?,
                SyncSubcommand::PullNonTransactions => engine.sync_pull_non_transactions().await?,
                SyncSubcommand::Push => engine.sync_push().await?,
                SyncSubcommand::All => engine.sync_all().await?,
            };

            println!("{}", json!({ "run_id": run_id }));
        }
        Commands::Local { command } => match command {
            LocalSubcommand::CategorizeApply { file } => {
                let content = tokio::fs::read_to_string(&file)
                    .await
                    .with_context(|| format!("failed to read decisions file: {file}"))?;
                let decisions = parse_decisions(&content)?;

                let mut applied = 0;
                for decision in decisions {
                    store.apply_local_category_decision(None, &decision).await?;
                    applied += 1;
                }

                println!("{}", json!({ "applied": applied, "file": file }));
            }
            LocalSubcommand::CategoryDelete {
                category_id,
                force,
                reason,
            } => {
                let change_id = store
                    .apply_local_category_delete(None, category_id, force, reason.as_deref())
                    .await?;
                println!(
                    "{}",
                    json!({
                        "enqueued": change_id,
                        "category_id": category_id,
                        "force": force
                    })
                );
            }
        },
        Commands::Audit { command } => match command {
            AuditSubcommand::List {
                run_id,
                entity,
                entity_id,
                limit,
            } => {
                audit::print_events(
                    &store,
                    run_id.as_deref(),
                    entity.as_deref(),
                    entity_id.as_deref(),
                    limit,
                )
                .await?;
            }
        },
        Commands::Outbox { command } => match command {
            OutboxSubcommand::Requeue { change_id } => {
                store.requeue_outbox_change(&change_id).await?;
                println!("{}", json!({ "requeued": change_id }));
            }
        },
    }

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .with_current_span(false)
        .with_span_list(false)
        .init();
}
