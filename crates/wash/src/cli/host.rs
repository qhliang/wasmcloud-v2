use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context as _;
use clap::Args;
use custom_plugin_blobstore::CustomBlobstore;
use custom_plugin_cf_d1::CloudflareD1;
use custom_plugin_kv::MultiBackendKeyValue;
use custom_plugin_llm_gateway::LlmGateway;
use tracing::info;
use wash_runtime::{
    engine::Engine,
    observability::Meters,
    plugin::{self},
};

use crate::cli::{CliCommand, CliContext, CommandOutput};

#[derive(Debug, Clone, Copy, Default, clap::ValueEnum)]
pub enum BlobstoreBackendType {
    #[default]
    Nats,
    Cloudflare,
}

#[derive(Debug, Clone, Args)]
pub struct HostCommand {
    /// The host group label to assign to the host
    #[arg(long = "host-group", default_value = "default")]
    pub host_group: String,

    /// NATS URL for Control Plane communications
    #[arg(long = "scheduler-nats-url", default_value = "nats://localhost:4222")]
    pub scheduler_nats_url: String,

    /// Path to TLS CA certificate file for NATS Scheduler connection
    #[arg(long = "scheduler-nats-tls-ca")]
    pub scheduler_nats_tls_ca: Option<PathBuf>,

    /// Enable TLS handshake first mode for NATS Scheduler connection
    #[arg(long = "scheduler-nats-tls-first", default_value_t = false)]
    pub scheduler_nats_tls_first: bool,

    /// Path to NATS TLS certificate file for NATS Scheduler connection
    #[arg(long = "scheduler-nats-tls-cert")]
    pub scheduler_nats_tls_cert: Option<PathBuf>,

    /// Path to NATS TLS private key file for NATS Scheduler connection
    #[arg(long = "scheduler-nats-tls-key")]
    pub scheduler_nats_tls_key: Option<PathBuf>,

    /// NATS URL for Data Plane communications
    #[arg(long = "data-nats-url", default_value = "nats://localhost:4222")]
    pub data_nats_url: String,

    /// The path to TLS CA certificate file for NATS Data connection
    #[arg(long = "data-nats-tls-ca")]
    pub data_nats_tls_ca: Option<PathBuf>,

    /// Enable TLS handshake first mode for NATS Data connection
    #[arg(long = "data-nats-tls-first", default_value_t = false)]
    pub data_nats_tls_first: bool,

    /// Path to NATS TLS certificate file for NATS Data connection
    #[arg(long = "data-nats-tls-cert")]
    pub data_nats_tls_cert: Option<PathBuf>,

    /// Path to NATS TLS private key file for NATS Data connection
    #[arg(long = "data-nats-tls-key")]
    pub data_nats_tls_key: Option<PathBuf>,

    /// The host name to assign to the host
    #[arg(long = "host-name")]
    pub host_name: Option<String>,

    /// The address on which the HTTP server will listen
    #[arg(long = "http-addr")]
    pub http_addr: Option<SocketAddr>,

    /// Enable WASI WebGPU support
    #[cfg(not(target_os = "windows"))]
    #[arg(long = "wasi-webgpu", default_value_t = false)]
    pub wasi_webgpu: bool,

    /// PostgreSQL connection URL for the wasmcloud:postgres plugin
    /// (e.g. postgres://user:pass@bouncer:6432?sslmode=require&pool_size=10)
    #[arg(long = "postgres-url", env = "WASH_POSTGRES_URL")]
    pub postgres_url: Option<String>,

    /// Allow insecure OCI Registries
    #[arg(long = "allow-insecure-registries", default_value_t = false)]
    pub allow_insecure_registries: bool,

    /// Timeout for pulling artifacts from OCI registries
    #[arg(long = "registry-pull-timeout", value_parser = humantime::parse_duration, default_value = "30s")]
    pub registry_pull_timeout: Duration,

    /// The directory to use for caching OCI artifacts
    #[arg(long = "oci-cache-dir")]
    pub oci_cache_dir: Option<PathBuf>,

    /// Enable WASI OpenTelemetry plugin
    #[arg(long = "wasi-otel", default_value_t = false)]
    pub wasi_otel: bool,

    /// The blobstore backend to use
    #[clap(long = "blobstore-backend", env = "BLOBSTORE_BACKEND")]
    pub blobstore_backend: BlobstoreBackendType,
}

impl CliCommand for HostCommand {
    async fn handle(&self, ctx: &CliContext) -> anyhow::Result<CommandOutput> {
        let scheduler_nats_client = wash_runtime::washlet::connect_nats(
            self.scheduler_nats_url.clone(),
            wash_runtime::washlet::NatsConnectionOptions {
                request_timeout: None,
                tls_ca: self.scheduler_nats_tls_ca.clone(),
                tls_first: self.scheduler_nats_tls_first,
                tls_cert: self.scheduler_nats_tls_cert.clone(),
                tls_key: self.scheduler_nats_tls_key.clone(),
            },
        )
        .await
        .context("failed to connect to NATS Scheduler URL")?;

        let data_nats_client = wash_runtime::washlet::connect_nats(
            self.data_nats_url.clone(),
            wash_runtime::washlet::NatsConnectionOptions {
                request_timeout: None,
                tls_ca: self.data_nats_tls_ca.clone(),
                tls_first: self.data_nats_tls_first,
                tls_cert: self.data_nats_tls_cert.clone(),
                tls_key: self.data_nats_tls_key.clone(),
            },
        )
        .await
        .context("failed to connect to NATS")?;
        let data_nats_client = Arc::new(data_nats_client);

        let host_config = wash_runtime::host::HostConfig {
            allow_oci_insecure: self.allow_insecure_registries,
            oci_pull_timeout: Some(self.registry_pull_timeout),
            oci_cache_dir: self.oci_cache_dir.clone(),
        };

        let engine = Engine::builder()
            .with_pooling_allocator(true)
            .with_fuel_consumption(ctx.enable_meters())
            .build()?;

        let mut cluster_host_builder = wash_runtime::washlet::ClusterHostBuilder::default()
            .with_engine(engine)
            .with_host_config(host_config)
            .with_nats_client(Arc::new(scheduler_nats_client))
            .with_host_group(self.host_group.clone())
            .with_plugin(Arc::new(plugin::wasi_config::DynamicConfig::new(true)))?
            .with_plugin(Arc::new(plugin::wasi_logging::TracingLogger::default()))?
            .with_plugin(Arc::new(plugin::wasmcloud_messaging::NatsMessaging::new(
                data_nats_client.clone(),
            )))?
            .with_meters(Meters::new(ctx.enable_meters()));

        // Enable multi-backend KV plugin
        cluster_host_builder =
            cluster_host_builder.with_plugin(Arc::new(MultiBackendKeyValue::new()))?;
        tracing::info!("Multi-backend KV plugin enabled");

        // Enable blobstore plugin
        match self.blobstore_backend {
            BlobstoreBackendType::Nats => {
                cluster_host_builder = cluster_host_builder.with_plugin(Arc::new(
                    plugin::wasi_blobstore::NatsBlobstore::new(&data_nats_client),
                ))?;
                tracing::info!("Nats blobstore plugin enabled");
            }
            BlobstoreBackendType::Cloudflare => {
                cluster_host_builder =
                    cluster_host_builder.with_plugin(Arc::new(CustomBlobstore::new()))?;
                tracing::info!("Cloudflare R2 blobstore plugin enabled");
            }
        }

        // Enable Cloudflare D1 plugin (always loaded by default)
        cluster_host_builder = cluster_host_builder.with_plugin(Arc::new(CloudflareD1::new()))?;
        tracing::info!("Cloudflare D1 plugin enabled");

        // Enable LLM gateway plugin
        cluster_host_builder = cluster_host_builder.with_plugin(Arc::new(LlmGateway::new()))?;
        tracing::info!("LLM gateway plugin enabled");

        // Enable crontab plugin
        cluster_host_builder =
            cluster_host_builder.with_plugin(Arc::new(custom_plugin_crontab::Crontab::new()))?;
        tracing::info!("Crontab plugin enabled");

        // Enable dingtalk stream plugin
        cluster_host_builder = cluster_host_builder
            .with_plugin(Arc::new(custom_plugin_dingtalk_stream::DingTalk::new()))?;
        tracing::info!("Dingtalk stream plugin enabled");

        // Enable feishu plugin
        cluster_host_builder =
            cluster_host_builder.with_plugin(Arc::new(custom_plugin_feishu::Feishu::new()))?;
        tracing::info!("Feishu plugin enabled");

        // Enable mail plugin
        cluster_host_builder =
            cluster_host_builder.with_plugin(Arc::new(custom_plugin_mail::Mail::new()))?;
        tracing::info!("Mail plugin enabled");

        // Enable codex plugin
        cluster_host_builder =
            cluster_host_builder.with_plugin(Arc::new(custom_plugin_codex::Codex::new()))?;
        tracing::info!("Codex plugin enabled");

        // Enable wechat plugin
        cluster_host_builder =
            cluster_host_builder.with_plugin(Arc::new(custom_plugin_wechat::Wechat::new()))?;
        tracing::info!("WeChat plugin enabled");

        // Enable telegram plugin
        cluster_host_builder =
            cluster_host_builder.with_plugin(Arc::new(custom_plugin_telegram::Telegram::new()))?;
        tracing::info!("Telegram plugin enabled");

        if let Some(postgres_url) = &self.postgres_url {
            cluster_host_builder = cluster_host_builder.with_plugin(Arc::new(
                plugin::wasmcloud_postgres::WasmcloudPostgres::new(postgres_url)
                    .context("failed to configure postgres plugin")?,
            ))?;
        }

        if let Some(host_name) = &self.host_name {
            cluster_host_builder = cluster_host_builder.with_host_name(host_name);
        }

        if let Some(addr) = self.http_addr {
            let http_router = wash_runtime::host::http::DynamicRouter::default();
            cluster_host_builder = cluster_host_builder.with_http_handler(Arc::new(
                wash_runtime::host::http::HttpServer::new(http_router, addr).await?,
            ));
        }

        // Enable otel plugin
        if self.wasi_otel {
            cluster_host_builder = cluster_host_builder
                .with_plugin(Arc::new(plugin::wasi_otel::WasiOtel::default()))?;
        }

        // Enable WASI WebGPU if requested
        #[cfg(not(target_os = "windows"))]
        if self.wasi_webgpu {
            tracing::info!("WASI WebGPU support enabled");
            cluster_host_builder = cluster_host_builder
                .with_plugin(Arc::new(plugin::wasi_webgpu::WebGpu::default()))?;
        }

        let cluster_host = cluster_host_builder
            .build()
            .context("failed to build cluster host")?;
        let host_cleanup = wash_runtime::washlet::run_cluster_host(cluster_host)
            .await
            .context("failed to start cluster node")?;

        tokio::signal::ctrl_c()
            .await
            .context("failed to listen for shutdown signal")?;

        info!("Stopping host...");

        host_cleanup.await?;

        Ok(CommandOutput::ok(
            "Host exited successfully".to_string(),
            None,
        ))
    }
}
