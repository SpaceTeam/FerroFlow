//! Contains code for logging telemetry and parameters to postgres.

use std::{
    sync::mpsc::{self, RecvTimeoutError},
    thread,
    time::Duration,
};

use anyhow::{Context, Result};
use diesel::prelude::*;

use crate::{
    db::timescale_schema::field_logs,
    events::{self, Event},
};

pub use self::models::FieldLog;

mod models;
mod schema;
mod timescale_schema;

pub fn spawn_logging_worker<'a>(
    database_url: String,
    event_dispatcher: &'a events::EventDispatcher,
    scope: &'a thread::Scope<'a, '_>,
) -> Result<()> {
    let mut conn =
        PgConnection::establish(&database_url).context("failed to connect to database")?;
    let (tx, rx) = mpsc::channel::<events::Event>();
    event_dispatcher.subscribe(tx);

    scope.spawn(move || {
        // Write to the db in batches for better performance
        let mut batch: Vec<FieldLog> = Vec::new();
        let batch_size_limit = 100;
        let flush_timeout = Duration::from_millis(100);

        loop {
            match rx.recv_timeout(flush_timeout) {
                Ok(Event::NodeFieldUpdated(log)) => {
                    batch.push(log);
                    if batch.len() < batch_size_limit {
                        continue;
                    }
                    if let Err(error) = flush_batch(&mut conn, &mut batch) {
                        eprintln!("Database logging worker error: {error:#}");
                    }
                }
                Ok(Event::Shutdown) => {
                    if !batch.is_empty() {
                        if let Err(error) = flush_batch(&mut conn, &mut batch) {
                            eprintln!("Database logging worker error: {error:#}");
                        }
                    }
                    println!("Worker shutting down gracefully.");
                    break;
                }
                Ok(_) => {
                    // Ignore other events
                }
                Err(RecvTimeoutError::Timeout) => {
                    if batch.is_empty() {
                        continue;
                    }
                    if let Err(error) = flush_batch(&mut conn, &mut batch) {
                        eprintln!("Database logging worker error: {error:#}");
                    }
                }
                Err(RecvTimeoutError::Disconnected) => {
                    if batch.is_empty() {
                        continue;
                    }
                    if let Err(error) = flush_batch(&mut conn, &mut batch) {
                        eprintln!("Database logging worker error: {error:#}");
                    }
                    println!("Worker shutting down gracefully.");
                    break;
                }
            }
        }
    });

    Ok(())
}

fn flush_batch(conn: &mut PgConnection, batch: &mut Vec<FieldLog>) -> Result<()> {
    diesel::insert_into(field_logs::table)
        .values(&*batch)
        .execute(conn)
        .with_context(|| format!("failed to flush {} logs to the database", batch.len()))?;

    batch.clear();
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use chrono::Utc;
    use diesel::{QueryDsl, RunQueryDsl, dsl::count_star};
    use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
    use serde_json::json;
    use testcontainers::{
        GenericImage,
        ImageExt,
        core::{IntoContainerPort, WaitFor},
        runners::SyncRunner,
    };

    use super::*;

    const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");

    #[test]
    fn flush_batch_inserts_logs_into_timescaledb() {
        let (_container, database_url) = start_timescaledb_test_database();
        let mut conn = connect_with_retry(&database_url);
        let mut batch = vec![FieldLog {
            timestamp: Utc::now(),
            node_id: 7,
            field_id: 11,
            field_name: "temperature".into(),
            field_value: json!(42),
        }];

        flush_batch(&mut conn, &mut batch).expect("batch insert should succeed");

        assert!(batch.is_empty());

        let row_count = field_logs::table
            .select(count_star())
            .first::<i64>(&mut conn)
            .expect("row count query should succeed");

        assert_eq!(row_count, 1);
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
}
