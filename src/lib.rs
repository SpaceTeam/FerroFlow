use crate::config::Config;
use crate::events::EventDispatcher;
use crate::nodes::NodeManager;

pub mod can;
pub mod config;
pub mod db;
pub mod events;
pub mod nodes;
pub mod sequence;
pub mod socket;

pub fn run_with_config(config: Config) -> anyhow::Result<()> {
    let event_dispatcher = events::EventDispatcher::new();

    let node_manager = nodes::NodeManager::new(&event_dispatcher);

    run_with_dependencies(&event_dispatcher, &node_manager, config)
}

pub fn run_with_dependencies(
    event_dispatcher: &EventDispatcher,
    node_manager: &NodeManager,
    config: Config,
) -> anyhow::Result<()> {
    let interfaces = config
        .can_bus_interfaces
        .iter()
        .map(|s| s.as_str())
        .collect::<Vec<&str>>();

    let _ = std::thread::scope::<'_, _, anyhow::Result<()>>(|scope| {
        can::spawn_can_threads(interfaces.as_slice(), event_dispatcher, scope)?;

        if !config.database_url.is_empty() {
            db::spawn_logging_worker(config.database_url.to_string(), event_dispatcher, scope)?;
        }

        if let Some(listen) = config
            .http_listen
            .as_ref()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
        {
            socket::http::spawn_http_server_thread(node_manager, event_dispatcher, listen, scope);
        }

        println!("Starting node registration");

        nodes::spawn_can_msg_handler_thread(node_manager, event_dispatcher, scope);
        nodes::spawn_heartbeat_thread(
            node_manager,
            std::time::Duration::from_secs(config.heartbeat_period),
            event_dispatcher,
            scope,
        );

        node_manager.start_node_registration();

        Ok(())
    });
    Ok(())
}
