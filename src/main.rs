use std::env;

use walle::{State, WALLE};

#[tokio::main]
async fn main() -> walle::Result<()> {
    walle::telemetry::init().await;

    // Initiatilize Kubernetes controller state
    let config_source = config::Config::builder()
        .add_source(config::File::with_name(env::var("APP_CONFIG").as_deref().unwrap_or(WALLE)))
        .add_source(config::Environment::with_prefix("APP"))
        .build()
        .unwrap();

    let settings = config_source.try_deserialize::<walle::config::AppConfig>().unwrap();

    let state = State::new(settings);
    let controller = walle::run(state.clone());

    controller.await;
    Ok(())
}
