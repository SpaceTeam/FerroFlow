use std::{
    io::{self, Read, Write},
    net::TcpStream,
};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

const DEFAULT_ADDR: &str = "127.0.0.1:8080";

fn main() -> Result<()> {
    let addr = parse_addr()?;

    println!("FerroFlow HTTP CLI targeting http://{addr}");
    print_help();

    loop {
        print!("ff-http> ");
        io::stdout().flush()?;

        let mut line = String::new();
        if io::stdin().read_line(&mut line)? == 0 {
            return Ok(());
        }

        match run_command(&addr, line.trim()) {
            Ok(CommandResult::Continue) => {}
            Ok(CommandResult::Quit) => return Ok(()),
            Err(error) => eprintln!("command error: {error:#}"),
        }
    }
}

fn parse_addr() -> Result<String> {
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
                println!("Usage: cargo run --bin ff-http-cli -- [--addr 127.0.0.1:8080]");
                std::process::exit(0);
            }
            other => addr = other.to_string(),
        }
    }

    addr = addr
        .strip_prefix("http://")
        .unwrap_or(&addr)
        .trim_end_matches('/')
        .to_string();

    if addr.is_empty() || addr.contains('/') {
        bail!("address must have the form host:port");
    }

    Ok(addr)
}

fn run_command(addr: &str, line: &str) -> Result<CommandResult> {
    let mut parts = line.split_whitespace();
    let Some(command) = parts.next() else {
        return Ok(CommandResult::Continue);
    };

    match command {
        "help" | "h" | "?" => print_help(),
        "quit" | "q" | "exit" => return Ok(CommandResult::Quit),
        "health" => execute_request(addr, "GET", "/health", None)?,
        "nodes" | "get-nodes" => execute_request(addr, "GET", "/api/nodes", None)?,
        "telemetry" | "get-telemetry" => execute_request(addr, "GET", "/api/telemetry", None)?,
        "get-name" => {
            let node_id = parse_node_id(&mut parts)?;
            let field_name = next_arg(&mut parts, "field name")?;
            let body = json!({
                "node_id": node_id,
                "field_name": field_name,
            });
            execute_request(addr, "POST", "/api/fields/read", Some(&body))?;
        }
        "get-id" => {
            let node_id = parse_node_id(&mut parts)?;
            let field_id = next_arg(&mut parts, "field ID")?
                .parse::<u8>()
                .with_context(|| "field ID must be an integer from 0 to 255")?;
            let body = json!({
                "node_id": node_id,
                "field_id": field_id,
            });
            execute_request(addr, "POST", "/api/fields/read", Some(&body))?;
        }
        "set-name" => {
            let node_id = parse_node_id(&mut parts)?;
            let parameter_name = next_arg(&mut parts, "parameter name")?;
            let value = parse_remaining_json(parts)?;
            let body = json!({
                "node_id": node_id,
                "parameter_name": parameter_name,
                "value": value,
            });
            execute_request(addr, "POST", "/api/parameters/set", Some(&body))?;
        }
        "set-id" => {
            let node_id = parse_node_id(&mut parts)?;
            let parameter_id = next_arg(&mut parts, "parameter ID")?
                .parse::<u8>()
                .with_context(|| "parameter ID must be an integer from 0 to 255")?;
            let value = parse_remaining_json(parts)?;
            let body = json!({
                "node_id": node_id,
                "parameter_id": parameter_id,
                "value": value,
            });
            execute_request(addr, "POST", "/api/parameters/set", Some(&body))?;
        }
        "request" => {
            let method = next_arg(&mut parts, "HTTP method")?.to_uppercase();
            let path = next_arg(&mut parts, "request path")?;
            let body_text = parts.collect::<Vec<_>>().join(" ");
            let body = if body_text.is_empty() {
                None
            } else {
                Some(
                    serde_json::from_str::<Value>(&body_text)
                        .with_context(|| "request body must be valid JSON")?,
                )
            };
            execute_request(addr, &method, path, body.as_ref())?;
        }
        other => bail!("unknown command {other}; type 'help'"),
    }

    Ok(CommandResult::Continue)
}

fn parse_node_id<'a>(parts: &mut impl Iterator<Item = &'a str>) -> Result<u8> {
    next_arg(parts, "node ID")?
        .parse::<u8>()
        .with_context(|| "node ID must be an integer from 0 to 255")
}

fn next_arg<'a>(
    parts: &mut impl Iterator<Item = &'a str>,
    description: &'static str,
) -> Result<&'a str> {
    parts
        .next()
        .with_context(|| format!("missing {description}"))
}

fn parse_remaining_json<'a>(parts: impl Iterator<Item = &'a str>) -> Result<Value> {
    let text = parts.collect::<Vec<_>>().join(" ");
    if text.is_empty() {
        bail!("missing JSON value");
    }

    serde_json::from_str(&text).with_context(|| "value must be valid JSON")
}

fn execute_request(addr: &str, method: &str, path: &str, body: Option<&Value>) -> Result<()> {
    if !path.starts_with('/') {
        bail!("request path must start with '/'");
    }

    let body = match body {
        Some(value) => serde_json::to_string(value)?,
        None => String::new(),
    };

    let mut stream = TcpStream::connect(addr)
        .with_context(|| format!("failed to connect to HTTP server at {addr}"))?;

    write!(
        stream,
        "{method} {path} HTTP/1.1\r\n\
         Host: {addr}\r\n\
         Accept: application/json\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len()
    )?;
    stream.flush()?;

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .with_context(|| "failed to read HTTP response")?;
    print_response(&response)
}

fn print_response(response: &[u8]) -> Result<()> {
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .with_context(|| "server returned an invalid HTTP response")?;

    let headers = std::str::from_utf8(&response[..header_end])
        .with_context(|| "HTTP response headers were not UTF-8")?;
    let status = headers.lines().next().unwrap_or("HTTP response");
    let body = &response[header_end + 4..];

    println!("{status}");
    if body.is_empty() {
        return Ok(());
    }

    match serde_json::from_slice::<Value>(body) {
        Ok(value) => println!("{}", serde_json::to_string_pretty(&value)?),
        Err(_) => println!("{}", String::from_utf8_lossy(body)),
    }

    Ok(())
}

fn print_help() {
    println!(
        r#"commands:
  health                              check whether FerroFlow is available
  nodes                               list registered nodes and parameters
  telemetry                           list all cached telemetry
  get-name <node> <name>              request a field by name
  get-id <node> <field-id>            request a field by ID
  set-name <node> <name> <json>       set a parameter by name
  set-id <node> <parameter-id> <json> set a parameter by ID
  request <method> <path> [json]      send an arbitrary HTTP request
  help                                show this help
  quit                                exit

examples:
  set-name 1 ignition true
  set-name 1 target_pressure 42.5
  set-id 1 2 "hello"
"#
    );
}

enum CommandResult {
    Continue,
    Quit,
}
