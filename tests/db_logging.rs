use std::{thread, time::Duration};

use chrono::Utc;
use diesel::{PgConnection, QueryableByName, RunQueryDsl, prelude::*};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use FerroFlow::{db, events};
use serde_json::json;
use testcontainers::{
    GenericImage,
    ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::SyncRunner,
};

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

#[test]
fn logging_worker_persists_events_to_timescaledb() {
    let (_container, database_url) = start_timescaledb_test_database();
    let event_dispatcher = events::EventDispatcher::new();

    std::thread::scope(|scope| {
        db::spawn_logging_worker(database_url.clone(), &event_dispatcher, scope)
            .expect("logging worker should start");

        event_dispatcher.dispatch(events::Event::NodeFieldUpdated(db::FieldLog {
            timestamp: Utc::now(),
            node_id: 3,
            field_id: 99,
            field_name: "tank_pressure".into(),
            field_value: json!(17.4),
        }));

        event_dispatcher.dispatch(events::Event::Shutdown);
    });

    let mut conn = connect_with_retry(&database_url);
    let row_count = diesel::sql_query("SELECT COUNT(*) AS count FROM field_logs")
        .get_result::<CountRow>(&mut conn)
        .expect("row count query should succeed");

    assert_eq!(row_count.count, 1);
}

fn start_timescaledb_test_database() -> (testcontainers::Container<GenericImage>, String) {
    let container = GenericImage::new("timescale/timescaledb", "latest-pg16")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_DB", "ferroflow_test")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .start()
        .expect("timescaledb container should start");

    let host = container
        .get_host()
        .expect("container host should be available")
        .to_string();
    let port = container
        .get_host_port_ipv4(5432)
        .expect("postgres port should be mapped");
    let database_url = format!(
        "postgres://postgres:postgres@{host}:{port}/ferroflow_test"
    );

    let mut conn = connect_with_retry(&database_url);
    conn.run_pending_migrations(MIGRATIONS)
        .expect("migrations should succeed");

    (container, database_url)
}

fn connect_with_retry(database_url: &str) -> PgConnection {
    for _ in 0..30 {
        match PgConnection::establish(database_url) {
            Ok(conn) => return conn,
            Err(_) => thread::sleep(Duration::from_millis(500)),
        }
    }

    panic!("failed to connect to test database at {database_url}");
}