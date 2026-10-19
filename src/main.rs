use anyhow::Context;
use ccpm_parser::{
    App, big_data_store::BigDataStore, config::Config, dictionary::DictionaryClient,
    host_directory::HostDirectoryClientHandle, http_server, kafka_consumer, kafka_sink::KafkaSink,
    mqtt_source, processing, registry::Registry, supervise,
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cfg = Arc::new(Config::from_env().context("loading configuration")?);
    tracing::info!(
        host = %cfg.mqtt.broker_host,
        port = cfg.mqtt.broker_port,
        tls = cfg.mqtt.ca_cert_pem.is_some(),
        auth = cfg.mqtt.username.is_some(),
        "resolved mqtt config"
    );

    let shutdown = CancellationToken::new();
    let registry = Arc::new(Registry::new());

    let kafka = Arc::new(KafkaSink::new(&cfg.kafka)?);
    let store = Arc::new(
        BigDataStore::new(&cfg.redis.url, cfg.redis.ca_cert_pem.as_deref())
            .await
            .context("connecting to redis")?,
    );

    let mut host_directory = HostDirectoryClientHandle::connect(&cfg.grpc.host_directory_address)
        .await
        .context("connecting to host-directory")?;
    let hosts = host_directory
        .fetch_active_hosts()
        .await
        .context("fetching active hosts from host-directory")?;
    tracing::info!(
        count = hosts.len(),
        "loaded imei -> hostId map from host-directory"
    );
    registry.load_hosts(hosts);

    let dictionary = DictionaryClient::new(&cfg.dictionary.base_url)?;
    let valid_keys = dictionary
        .fetch_keys(processing::BOARD_TYPE)
        .await
        .context("fetching valid keys from dictionary")?;
    tracing::info!(
        count = valid_keys.len(),
        "loaded valid keys from dictionary"
    );
    registry.load_valid_keys(valid_keys);

    let app = App::new(cfg, registry, kafka, store);

    // The probe server needs the dependencies it reports on, so it starts
    // after them: the port is closed until boot finishes, which a k8s
    // startupProbe is there to absorb.
    let http_task = supervise("http", shutdown.clone(), {
        let app = app.clone();
        let shutdown = shutdown.clone();
        let port = app.cfg.http_port;
        async move { http_server::run(app, shutdown, port).await }
    });

    let kafka_consumer_task = supervise("kafka-consumer", shutdown.clone(), {
        let app = app.clone();
        let shutdown = shutdown.clone();
        async move { kafka_consumer::run(&app.cfg.kafka, shutdown, app.registry.clone()).await }
    });

    let mqtt_task = supervise("mqtt", shutdown.clone(), {
        let app = app.clone();
        let shutdown = shutdown.clone();
        async move { mqtt_source::run(app, shutdown).await }
    });

    // Either the operator stops us, or a component died and cancelled the
    // token on its way out.
    let stopped_by_task = tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result.context("waiting for ctrl-c")?;
            tracing::info!("shutdown signal received");
            false
        }
        () = shutdown.cancelled() => {
            tracing::error!("a component stopped, shutting down");
            true
        }
    };

    shutdown.cancel();
    let _ = tokio::join!(mqtt_task, kafka_consumer_task, http_task);

    // Nothing new can be enqueued now, so this drains rather than races.
    app.kafka.wait_for_inflight().await;
    tracing::info!("shutdown complete");

    if stopped_by_task {
        anyhow::bail!("exiting because a component stopped unexpectedly");
    }
    Ok(())
}
