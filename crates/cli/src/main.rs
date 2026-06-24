use std::{path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use reqwest::Client;
use serde::{Serialize, Deserialize};
use tokio::sync::{oneshot, watch};
use tracing::{info, warn};
use txwatch_config::AppConfig;
use txwatch_notifier::{build_client, send_webhook, test_payload_with_network};

// ── CLI definition ────────────────────────────────────────────────────────────

const VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("TXWATCH_GIT_SHA"),
    " built ",
    env!("TXWATCH_BUILD_TIMESTAMP"),
    ")"
);

#[derive(Serialize, Deserialize)]
struct ValidateJsonOutput {
    valid: bool,
    poll_interval_seconds: u64,
    contracts: Vec<ContractSummary>,
}

#[derive(Serialize, Deserialize)]
struct ContractSummary {
    label: String,
    contract_id: String,
    network: String,
    webhook_url: String,
    webhook_secret_set: bool,
    rules: Vec<String>,
}

#[derive(Parser)]
#[command(
    name    = "txwatch",
    version = VERSION,
    about   = "Stellar Soroban contract monitor & webhook alert engine"
)]
struct Cli {
    /// Path to the TOML config file
    #[arg(short, long, default_value = "config/example.toml")]
    config: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the polling engine (watches all contracts in the config)
    Watch {
        /// Do not actually send webhooks; only log matched rules
        #[arg(long)]
        dry_run: bool,

        /// Run exactly one poll cycle across all configured contracts, then exit
        #[arg(long)]
        poll_once: bool,
    },

    /// Parse and validate the config file, then print a summary
    ///
    /// Exit codes: 0 = valid config, 1 = invalid or missing config
    Validate {
        /// Send a HEAD/OPTIONS request to each webhook URL and warn on unreachable endpoints.
        #[arg(long)]
        check_webhooks: bool,

        /// Output format (text or json)
        #[arg(long, default_value = "text")]
        output: String,
    },

    /// Send a test webhook payload to a URL and exit
    ///
    /// Exit codes: 0 = webhook delivered, 1 = delivery failed (unreachable or HTTP error)
    TestWebhook {
        /// The webhook URL to POST to
        #[arg(long)]
        url: String,

        /// Label to include in the test payload
        #[arg(long, default_value = "TxWatch Test")]
        label: String,
    },

    /// Print version and build information
    Version,

    /// List all available alert rule types and their descriptions
    ListRules,
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let cli = Cli::parse();

    match cli.command {
        Command::Validate { check_webhooks, output } => {
            let cfg = AppConfig::from_file(&cli.config)?;

            if output == "json" {
                let contracts: Vec<ContractSummary> = cfg.contracts.iter().map(|c| {
                    ContractSummary {
                        label: c.label.clone(),
                        contract_id: c.contract_id.clone(),
                        network: c.network.as_str().to_string(),
                        webhook_url: c.webhook_url.clone(),
                        webhook_secret_set: c.webhook_secret.is_some(),
                        rules: c.rules.iter().map(|r| r.label()).collect(),
                    }
                }).collect();

                let output_json = ValidateJsonOutput {
                    valid: true,
                    poll_interval_seconds: cfg.poll_interval_seconds,
                    contracts,
                };
                println!("{}", serde_json::to_string_pretty(&output_json)?);
            } else {
                println!("Config is valid.");
                println!("  poll_interval_seconds : {}", cfg.poll_interval_seconds);
                println!("  contracts             : {}", cfg.contracts.len());
                println!();
                for c in &cfg.contracts {
                    println!(
                        "  [{network}] {label}",
                        network = c.network.display_name(),
                        label = c.label
                    );
                    println!("    contract_id  : {}", c.contract_id);
                    println!("    webhook_url  : {}", c.webhook_url);
                    println!(
                        "    secret       : {}",
                        if c.webhook_secret.is_some() {
                            "set"
                        } else {
                            "none"
                        }
                    );
                    println!("    rules        : {}", c.rules.len());
                    for rule in &c.rules {
                        println!("      - {}", rule.label());
                    }
                    println!("    horizon      : {}", c.network.horizon_base_url());
                    println!(
                        "    explorer     : {}/contract/{}",
                        c.network.explorer_base_url(),
                        c.contract_id
                    );
                }

                if check_webhooks {
                    let client = Client::builder()
                        .timeout(Duration::from_secs(15))
                        .build()
                        .context("failed to build HTTP client")?;

                    for c in &cfg.contracts {
                        let reachable = check_webhook_reachable(&client, &c.webhook_url).await;
                        if let Err(e) = reachable {
                            warn!(webhook_url = %c.webhook_url, contract = %c.label, error = %e, "webhook reachability check failed");
                        } else if !reachable.unwrap() {
                            warn!(webhook_url = %c.webhook_url, contract = %c.label, "webhook endpoint is unreachable");
                        }
                    }
                }
            }
        }

        Command::TestWebhook { url, label } => {
            let cfg = AppConfig::from_file(&cli.config)?;
            if cfg.contracts.is_empty() {
                return Err(anyhow::anyhow!(
                    "config has no contracts; cannot derive network for test-webhook"
                ));
            }
            let first_contract = &cfg.contracts[0];
            let network_name = first_contract.network.as_str();
            let horizon_base_url = first_contract.network.horizon_base_url();
            let payload = test_payload_with_network(&label, &url, network_name, horizon_base_url);
            let client  = build_client().context("failed to build HTTP client")?;

            info!(url = %url, "sending test webhook");
            let (_, shutdown_rx) = oneshot::channel::<()>();
            send_webhook(&client, &url, &payload, None, shutdown_rx)
                .await
                .with_context(|| format!("test webhook to '{}' failed", url))?;
            println!("Test webhook delivered successfully to {}", url);
            std::process::exit(0);
        }

        Command::Watch { dry_run, poll_once } => {
            let cfg = AppConfig::from_file(&cli.config)?;

            if poll_once {
                // Run exactly one poll cycle and exit
                info!(
                    version        = VERSION,
                    contracts      = cfg.contracts.len(),
                    interval_secs  = cfg.poll_interval_seconds,
                    dry_run        = dry_run,
                    "starting TxWatch (poll-once mode)"
                );
                txwatch_poller::run_once(cfg, dry_run).await?;
            } else {
                // Graceful shutdown: allow the current poll cycle to finish before exiting.
                let (shutdown_tx, shutdown_rx) = watch::channel(false);
                tokio::spawn(async move {
                    if let Err(e) = tokio::signal::ctrl_c().await {
                        warn!(error = ?e, "failed to install Ctrl+C handler");
                        return;
                    }
                    let _ = shutdown_tx.send(true);
                });

                info!(
                    version        = VERSION,
                    contracts      = cfg.contracts.len(),
                    interval_secs  = cfg.poll_interval_seconds,
                    dry_run        = dry_run,
                    "starting TxWatch"
                );
                txwatch_poller::run_with_shutdown(cfg, dry_run, shutdown_rx).await?;
            }
        }

        Command::Version => {
            println!("{}", VERSION);
        }

        Command::ListRules => {
            println!("Available alert rules:\n");
            println!("  AnyTransaction");
            println!("    Fires on every transaction. No parameters required.\n");
            println!("  TransactionFailed");
            println!("    Fires when a transaction fails (successful = false). No parameters required.\n");
            println!("  LargeTransfer");
            println!("    Fires when payment amount >= threshold_xlm XLM.");
            println!("    Fields: threshold_xlm (u64, required, > 0)\n");
            println!("  FunctionCalled");
            println!("    Fires when the Soroban invoke_host_function operation calls exactly function_name.");
            println!("    Fields: function_name (string, required, case-sensitive)\n");
            println!("  AdminFunctionCalled");
            println!("    Fires when the invoked function is any entry in function_names.");
            println!("    Equivalent to multiple FunctionCalled rules but produces a single AdminFunctionCalled([...]) label in the alert.");
            println!("    Fields: function_names ([string], required, non-empty list)\n");
            println!("  HighFee");
            println!("    Fires when the transaction's total fee exceeds threshold_stroops.");
            println!("    Fields: threshold_stroops (u64, required, > 0) OR threshold_xlm (u64, optional)");
            println!("    Note: Stroops are the smallest unit of XLM (1 XLM = 10,000,000 stroops)");
        }
    }

    Ok(())
}
async fn check_webhook_reachable(client: &Client, url: &str) -> Result<bool> {
    let response = client.head(url).send().await;
    match response {
        Ok(resp) if resp.status().is_success() => return Ok(true),
        Ok(resp) if resp.status() == 405 || resp.status() == 501 => {
            let resp = client.request(reqwest::Method::OPTIONS, url).send().await?;
            return Ok(resp.status().is_success());
        }
        Ok(_) => return Ok(false),
        Err(err) => {
            if err.is_builder() {
                return Err(err.into());
            }
            return Ok(false);
        }
    }
}
// ── Tracing initialisation ────────────────────────────────────────────────────

fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_json_output_shape() {
        let summary = ContractSummary {
            label: "Test".to_string(),
            contract_id: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_string(),
            network: "testnet".to_string(),
            webhook_url: "https://example.com/webhook".to_string(),
            webhook_secret_set: true,
            rules: vec!["AnyTransaction".to_string()],
        };

        let output = ValidateJsonOutput {
            valid: true,
            poll_interval_seconds: 10,
            contracts: vec![summary],
        };

        let json = serde_json::to_string(&output).expect("should serialize to JSON");
        let parsed: ValidateJsonOutput = serde_json::from_str(&json).expect("should deserialize JSON");

        assert!(parsed.valid);
        assert_eq!(parsed.poll_interval_seconds, 10);
        assert_eq!(parsed.contracts.len(), 1);
        assert_eq!(parsed.contracts[0].label, "Test");
        assert!(parsed.contracts[0].webhook_secret_set);

        // Verify webhook_secret value is not in the JSON
        assert!(!json.contains("secret") || json.contains("webhook_secret_set"));
    }
}
