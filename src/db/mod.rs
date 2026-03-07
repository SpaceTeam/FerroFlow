//! Contains code for logging telemetry and parameters to postgres.

use std::{
    sync::mpsc::{self, RecvTimeoutError},
    thread,
    time::Duration,
};

use anyhow::{Context, Result};
use diesel::prelude::*;
use models::SensorLog;

use crate::db::schema::sensor_logs;

mod models;
mod schema;

pub fn spawn_logging_worker(
    database_url: String,
) -> Result<(mpsc::Sender<SensorLog>, thread::JoinHandle<()>)> {
    let (tx, rx) = mpsc::channel::<SensorLog>();
    let mut conn =
        PgConnection::establish(&database_url).context("failed to connect to database")?;
    let handle = thread::spawn(move || {
        // Write to the db in batches for better performance
        let mut batch: Vec<SensorLog> = Vec::new();
        let batch_size_limit = 100;
        let flush_timeout = Duration::from_millis(100);

        loop {
            match rx.recv_timeout(flush_timeout) {
                Ok(log) => {
                    batch.push(log);
                    if batch.len() >= batch_size_limit {
                        if let Err(error) = flush_batch(&mut conn, &mut batch) {
                            eprintln!("Database logging worker error: {error:#}");
                        }
                    }
                }
                Err(RecvTimeoutError::Timeout) => {
                    if !batch.is_empty() {
                        if let Err(error) = flush_batch(&mut conn, &mut batch) {
                            eprintln!("Database logging worker error: {error:#}");
                        }
                    }
                }
                Err(RecvTimeoutError::Disconnected) => {
                    if !batch.is_empty() {
                        if let Err(error) = flush_batch(&mut conn, &mut batch) {
                            eprintln!("Database logging worker error: {error:#}");
                        }
                    }
                    println!("Worker shutting down gracefully.");
                    break;
                }
            }
        }
    });

    Ok((tx, handle))
}

fn flush_batch(conn: &mut PgConnection, batch: &mut Vec<SensorLog>) -> Result<()> {
    diesel::insert_into(sensor_logs::table)
        .values(&*batch)
        .execute(conn)
        .with_context(|| format!("failed to flush {} logs to the database", batch.len()))?;

    batch.clear();
    Ok(())
}
