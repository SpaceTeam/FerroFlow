use std::{
    io::{self, ErrorKind, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::mpsc::{self, TryRecvError},
    thread,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

const DEFAULT_ADDR: &str = "127.0.0.1:8080";
const POLL_PERIOD: Duration = Duration::from_millis(50);

fn main() -> Result<()> {
    let addr = parse_addr()?;
    let listener = TcpListener::bind(addr).with_context(|| format!("failed to bind {addr}"))?;
    listener.set_nonblocking(true)?;

    println!("FerroFlow socket CLI listening on {addr}");
    print_help();

    let (tx, rx) = mpsc::channel();
    thread::spawn(move || stdin_thread(tx));

    loop {
        match wait_for_connection(&listener, &rx)? {
            WaitResult::Connected(stream) => {
                println!("connected: {}", stream.peer_addr()?);
                match connection_loop(stream, &rx)? {
                    ConnectionResult::Disconnected => {
                        println!("disconnected; waiting for FerroFlow to reconnect");
                    }
                    ConnectionResult::Quit => return Ok(()),
                }
            }
            WaitResult::Quit => return Ok(()),
        }
    }
}

fn parse_addr() -> Result<SocketAddr> {
    let mut args = std::env::args().skip(1);
    let mut addr = DEFAULT_ADDR.to_string();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-a" | "--addr" => {
                addr = args
                    .next()
                    .with_context(|| format!("{arg} requires an address"))?;
            }
            "-h" | "--help" => {
                println!("Usage: cargo run --bin ff-socket-cli -- [--addr 127.0.0.1:8080]");
                std::process::exit(0);
            }
            other => addr = other.to_string(),
        }
    }

    addr.parse()
        .with_context(|| format!("invalid socket address {addr}"))
}

fn stdin_thread(tx: mpsc::Sender<CliCommand>) {
    let stdin = io::stdin();
    loop {
        print!("ff> ");
        let _ = io::stdout().flush();

        let mut line = String::new();
        match stdin.read_line(&mut line) {
            Ok(0) => {
                let _ = tx.send(CliCommand::Quit);
                return;
            }
            Ok(_) => match parse_command(line.trim()) {
                Ok(Some(command)) => {
                    if tx.send(command).is_err() {
                        return;
                    }
                }
                Ok(None) => {}
                Err(error) => eprintln!("command error: {error:#}"),
            },
            Err(error) => {
                eprintln!("stdin error: {error}");
                let _ = tx.send(CliCommand::Quit);
                return;
            }
        }
    }
}

fn parse_command(line: &str) -> Result<Option<CliCommand>> {
    let mut parts = line.split_whitespace();
    let Some(command) = parts.next() else {
        return Ok(None);
    };

    match command {
        "help" | "h" | "?" => Ok(Some(CliCommand::Help)),
        "quit" | "q" | "exit" => Ok(Some(CliCommand::Quit)),
        "nodes" | "get-nodes" => Ok(Some(CliCommand::Send(json!({
            "type": "get_nodes",
            "content": {}
        })))),
        "telemetry" | "get-telemetry" => Ok(Some(CliCommand::Send(json!({
            "type": "get_telemetry",
            "content": {}
        })))),
        "get-mapped" => {
            let name = next_arg(&mut parts, "mapped field name")?;
            Ok(Some(CliCommand::Send(json!({
                "type": "get_field",
                "content": {
                    "field": {
                        "type": "mapped",
                        "name": name
                    }
                }
            }))))
        }
        "get-raw" => {
            let node_name = next_arg(&mut parts, "node name")?;
            let field_name = next_arg(&mut parts, "raw field name")?;
            Ok(Some(CliCommand::Send(json!({
                "type": "get_field",
                "content": {
                    "field": {
                        "type": "raw",
                        "node_name": node_name,
                        "field_name": field_name
                    }
                }
            }))))
        }
        "set-mapped" => {
            let name = next_arg(&mut parts, "mapped parameter name")?;
            let value = next_arg(&mut parts, "numeric value")?
                .parse::<f64>()
                .with_context(|| "mapped parameter value must be numeric")?;
            Ok(Some(CliCommand::Send(json!({
                "type": "set_parameter",
                "content": {
                    "field": {
                        "type": "mapped",
                        "name": name
                    },
                    "value": value
                }
            }))))
        }
        "set-raw" => {
            let node_name = next_arg(&mut parts, "node name")?;
            let field_name = next_arg(&mut parts, "raw field name")?;
            let value_text = parts.collect::<Vec<_>>().join(" ");
            if value_text.is_empty() {
                bail!("missing JSON value");
            }
            let value = serde_json::from_str::<Value>(&value_text)
                .with_context(|| "raw parameter value must be valid JSON")?;
            Ok(Some(CliCommand::Send(json!({
                "type": "set_parameter",
                "content": {
                    "field": {
                        "type": "raw",
                        "node_name": node_name,
                        "field_name": field_name
                    },
                    "value": value
                }
            }))))
        }
        "send-json" | "send" => {
            let message = line.strip_prefix(command).unwrap_or_default().trim_start();
            if message.is_empty() {
                bail!("missing JSON message");
            }
            Ok(Some(CliCommand::Send(
                serde_json::from_str(message).with_context(|| "invalid JSON message")?,
            )))
        }
        other => bail!("unknown command {other}; type 'help'"),
    }
}

fn next_arg<'a>(
    parts: &mut impl Iterator<Item = &'a str>,
    description: &'static str,
) -> Result<&'a str> {
    parts
        .next()
        .with_context(|| format!("missing {description}"))
}

fn wait_for_connection(
    listener: &TcpListener,
    rx: &mpsc::Receiver<CliCommand>,
) -> Result<WaitResult> {
    loop {
        match listener.accept() {
            Ok((stream, _)) => return Ok(WaitResult::Connected(stream)),
            Err(error) if error.kind() == ErrorKind::WouldBlock => {}
            Err(error) => return Err(error).with_context(|| "failed to accept connection"),
        }

        match rx.try_recv() {
            Ok(CliCommand::Quit) => return Ok(WaitResult::Quit),
            Ok(CliCommand::Help) => print_help(),
            Ok(CliCommand::Send(_)) => {
                eprintln!("not connected yet; command was ignored");
            }
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => return Ok(WaitResult::Quit),
        }

        thread::sleep(POLL_PERIOD);
    }
}

fn connection_loop(
    mut stream: TcpStream,
    rx: &mpsc::Receiver<CliCommand>,
) -> Result<ConnectionResult> {
    stream.set_nonblocking(true)?;
    let mut buffer = Vec::new();

    loop {
        match read_messages(&mut stream, &mut buffer)? {
            ReadState::Open(messages) => {
                for message in messages {
                    print_received_message(&message);
                }
            }
            ReadState::Closed => return Ok(ConnectionResult::Disconnected),
        }

        loop {
            match rx.try_recv() {
                Ok(CliCommand::Send(message)) => {
                    send_message(&mut stream, &message)?;
                    println!("<- {}", compact_json(&message));
                }
                Ok(CliCommand::Help) => print_help(),
                Ok(CliCommand::Quit) => return Ok(ConnectionResult::Quit),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Ok(ConnectionResult::Quit),
            }
        }

        thread::sleep(POLL_PERIOD);
    }
}

fn read_messages(stream: &mut TcpStream, buffer: &mut Vec<u8>) -> Result<ReadState> {
    let mut scratch = [0_u8; 4096];
    loop {
        match stream.read(&mut scratch) {
            Ok(0) => return Ok(ReadState::Closed),
            Ok(bytes_read) => buffer.extend_from_slice(&scratch[..bytes_read]),
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == ErrorKind::WouldBlock => break,
            Err(error) => return Err(error).with_context(|| "failed to read socket"),
        }
    }

    let mut messages = Vec::new();
    while buffer.len() >= 2 {
        let len = u16::from_be_bytes([buffer[0], buffer[1]]) as usize;
        if buffer.len() < len + 2 {
            break;
        }

        messages.push(buffer[2..len + 2].to_vec());
        buffer.drain(..len + 2);
    }

    Ok(ReadState::Open(messages))
}

fn send_message(stream: &mut TcpStream, message: &Value) -> Result<()> {
    let payload = serde_json::to_vec(message)?;
    let len = u16::try_from(payload.len())
        .with_context(|| format!("message too large: {} bytes", payload.len()))?;

    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(&payload)?;
    Ok(())
}

fn print_received_message(message: &[u8]) {
    match serde_json::from_slice::<Value>(message) {
        Ok(value) => println!("-> {}", pretty_json(&value)),
        Err(error) => {
            println!("-> <invalid json: {error}>");
            println!("{}", String::from_utf8_lossy(message));
        }
    }
}

fn pretty_json(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

fn compact_json(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| value.to_string())
}

fn print_help() {
    println!(
        r#"commands:
  nodes                         request current nodes
  telemetry                     request full telemetry snapshot
  get-mapped <name>             request mapped field value
  get-raw <node> <field>        request raw field by device_name + raw field name
  set-mapped <name> <value>     set mapped parameter using a numeric mapped value
  set-raw <node> <field> <json> set raw parameter using a JSON value
  send-json <json>              send a complete protocol message
  help                          show this help
  quit                          exit
"#
    );
}

enum CliCommand {
    Send(Value),
    Help,
    Quit,
}

enum WaitResult {
    Connected(TcpStream),
    Quit,
}

enum ConnectionResult {
    Disconnected,
    Quit,
}

enum ReadState {
    Open(Vec<Vec<u8>>),
    Closed,
}
