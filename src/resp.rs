// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! Redis-compatible (RESP) front end. Lets existing Redis clients, libraries, and tools (redis-cli,
//! redis-py, ioredis, …) talk to stplr without change for the common command set. Maps RESP commands
//! onto the same shard store as the HTTP + binary protocols; keys live in one flat collection
//! (`kv`), matching Redis's single keyspace.
//!
//! Supported: PING, AUTH, SELECT, GET, SET (+EX/PX), GETSET, DEL, EXISTS, INCR/INCRBY, DECR/DECRBY,
//! SADD, SREM, SMEMBERS, SCARD, SISMEMBER, SCAN (cursor/MATCH/COUNT), TYPE, DBSIZE, COMMAND, CLIENT,
//! INFO, QUIT. Sets are stored as JSON arrays (so GET on a set key returns its JSON form rather than
//! erroring WRONGTYPE — a documented simplification).

use std::sync::Arc;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};

use crate::net::SharedShard;

/// The single keyspace RESP clients see (Redis has no collections).
const COLL: &str = "kv";

/// Bind and serve the RESP protocol on `addr`. `token`, if set, requires `AUTH <token>` first.
pub async fn serve_addr(addr: std::net::SocketAddr, state: SharedShard, token: Option<String>) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    serve(listener, state, token).await;
    Ok(())
}

pub async fn serve(listener: TcpListener, state: SharedShard, token: Option<String>) {
    let token = Arc::new(token);
    loop {
        let (sock, _) = match listener.accept().await {
            Ok(x) => x,
            Err(_) => continue,
        };
        sock.set_nodelay(true).ok();
        let st = state.clone();
        let token = token.clone();
        tokio::spawn(async move {
            let _ = handle(sock, st, token).await;
        });
    }
}

async fn handle(sock: TcpStream, st: SharedShard, token: Arc<Option<String>>) -> std::io::Result<()> {
    let (rd, wr) = sock.into_split();
    let mut r = BufReader::new(rd);
    let mut w = BufWriter::new(wr);
    let mut authed = token.is_none();
    loop {
        let args = match read_command(&mut r).await? {
            Some(a) if !a.is_empty() => a,
            Some(_) => continue, // empty line
            None => break,       // EOF
        };
        let cmd = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
        let mut out = Vec::new();

        // AUTH is always allowed; until authenticated only PING/QUIT/AUTH are.
        if !authed && !matches!(cmd.as_str(), "AUTH" | "PING" | "QUIT" | "HELLO") {
            err(&mut out, "NOAUTH Authentication required.");
            w.write_all(&out).await?;
            w.flush().await?;
            continue;
        }

        match cmd.as_str() {
            "PING" => {
                if args.len() > 1 {
                    bulk(&mut out, Some(&args[1]));
                } else {
                    simple(&mut out, "PONG");
                }
            }
            "AUTH" => match token.as_deref() {
                Some(t) if args.len() >= 2 && args.last().map(|p| p == t.as_bytes()).unwrap_or(false) => {
                    authed = true;
                    simple(&mut out, "OK");
                }
                Some(_) => err(&mut out, "WRONGPASS invalid username-password pair"),
                None => err(&mut out, "ERR Client sent AUTH, but no password is set"),
            },
            "QUIT" => {
                simple(&mut out, "OK");
                w.write_all(&out).await?;
                w.flush().await?;
                break;
            }
            "SELECT" | "CLIENT" => simple(&mut out, "OK"),
            "HELLO" => err(&mut out, "ERR unknown command 'HELLO'"), // clients fall back to RESP2
            "COMMAND" => array_header(&mut out, 0),
            "INFO" => bulk(&mut out, Some(b"# Server\r\nstplr_resp:1\r\n")),
            "DBSIZE" => {
                let keys = st.read().unwrap().scan_range(COLL, None, None, None, usize::MAX);
                int(&mut out, keys.len() as i64);
            }

            "GET" if args.len() == 2 => {
                let v = st.read().unwrap().object(COLL, &s(&args[1]));
                bulk_value(&mut out, v.as_ref());
            }
            "SET" if args.len() >= 3 => {
                let key = s(&args[1]);
                let val = Value::String(s(&args[2]));
                // optional EX <sec> / PX <ms>
                let mut ttl_ms = 0u64;
                let mut i = 3;
                while i + 1 < args.len() {
                    match String::from_utf8_lossy(&args[i]).to_ascii_uppercase().as_str() {
                        "EX" => ttl_ms = s(&args[i + 1]).parse::<u64>().unwrap_or(0) * 1000,
                        "PX" => ttl_ms = s(&args[i + 1]).parse::<u64>().unwrap_or(0),
                        _ => {}
                    }
                    i += 2;
                }
                if ttl_ms > 0 {
                    st.write().unwrap().write_object_ttl(COLL, &key, val, ttl_ms);
                } else {
                    st.write().unwrap().write_object(COLL, &key, val);
                }
                simple(&mut out, "OK");
            }
            "GETSET" if args.len() == 3 => {
                let key = s(&args[1]);
                let mut g = st.write().unwrap();
                let old = g.object(COLL, &key);
                g.write_object(COLL, &key, Value::String(s(&args[2])));
                drop(g);
                bulk_value(&mut out, old.as_ref());
            }
            "DEL" if args.len() >= 2 => {
                let mut n = 0i64;
                let mut g = st.write().unwrap();
                for k in &args[1..] {
                    let key = s(k);
                    if g.object(COLL, &key).is_some() {
                        g.delete_object(COLL, &key);
                        n += 1;
                    }
                }
                int(&mut out, n);
            }
            "EXISTS" if args.len() >= 2 => {
                let g = st.read().unwrap();
                let n = args[1..].iter().filter(|k| g.object(COLL, &s(k)).is_some()).count();
                int(&mut out, n as i64);
            }
            "INCR" | "DECR" if args.len() == 2 => {
                let d = if cmd == "INCR" { 1 } else { -1 };
                match st.write().unwrap().incr(COLL, &s(&args[1]), d) {
                    Some(v) => int(&mut out, v),
                    None => err(&mut out, "ERR value is not an integer or out of range"),
                }
            }
            "INCRBY" | "DECRBY" if args.len() == 3 => {
                let by: i64 = match s(&args[2]).parse() {
                    Ok(v) => v,
                    Err(_) => {
                        err(&mut out, "ERR value is not an integer or out of range");
                        w.write_all(&out).await?;
                        w.flush().await?;
                        continue;
                    }
                };
                let d = if cmd == "INCRBY" { by } else { -by };
                match st.write().unwrap().incr(COLL, &s(&args[1]), d) {
                    Some(v) => int(&mut out, v),
                    None => err(&mut out, "ERR value is not an integer or out of range"),
                }
            }

            "SADD" if args.len() >= 3 => {
                let key = s(&args[1]);
                let mut added = 0i64;
                let mut g = st.write().unwrap();
                for m in &args[2..] {
                    if g.set_add(COLL, &key, &s(m)) {
                        added += 1;
                    }
                }
                int(&mut out, added);
            }
            "SREM" if args.len() >= 3 => {
                let key = s(&args[1]);
                let mut removed = 0i64;
                let mut g = st.write().unwrap();
                for m in &args[2..] {
                    if g.set_remove(COLL, &key, &s(m)) {
                        removed += 1;
                    }
                }
                int(&mut out, removed);
            }
            "SMEMBERS" if args.len() == 2 => {
                let members = set_members(&st, &s(&args[1]));
                array_header(&mut out, members.len());
                for m in members {
                    bulk(&mut out, Some(m.as_bytes()));
                }
            }
            "SCARD" if args.len() == 2 => int(&mut out, set_members(&st, &s(&args[1])).len() as i64),
            "SISMEMBER" if args.len() == 3 => {
                let has = set_members(&st, &s(&args[1])).contains(&s(&args[2]));
                int(&mut out, has as i64);
            }

            "SCAN" if args.len() >= 2 => {
                // SCAN <cursor> [MATCH prefix*] [COUNT n]. Cursor "0" = start; we return the last
                // key as the next cursor (opaque), "0" when drained.
                let cursor = s(&args[1]);
                let after = if cursor == "0" { None } else { Some(cursor.clone()) };
                let mut count = 10usize;
                let mut prefix: Option<String> = None;
                let mut i = 2;
                while i + 1 < args.len() {
                    match String::from_utf8_lossy(&args[i]).to_ascii_uppercase().as_str() {
                        "COUNT" => count = s(&args[i + 1]).parse().unwrap_or(10),
                        "MATCH" => {
                            let p = s(&args[i + 1]);
                            prefix = Some(p.trim_end_matches('*').to_string());
                        }
                        _ => {}
                    }
                    i += 2;
                }
                let keys = st.read().unwrap().scan_range(COLL, after.as_deref(), prefix.as_deref(), None, count);
                let next = if keys.len() < count { "0".to_string() } else { keys.last().cloned().unwrap_or_else(|| "0".into()) };
                array_header(&mut out, 2);
                bulk(&mut out, Some(next.as_bytes()));
                array_header(&mut out, keys.len());
                for k in keys {
                    bulk(&mut out, Some(k.as_bytes()));
                }
            }
            "TYPE" if args.len() == 2 => {
                let v = st.read().unwrap().object(COLL, &s(&args[1]));
                let t = match v {
                    None => "none",
                    Some(Value::Array(_)) => "set",
                    Some(_) => "string",
                };
                simple(&mut out, t);
            }

            _ => err(&mut out, &format!("ERR unknown command '{}'", cmd.to_ascii_lowercase())),
        }

        w.write_all(&out).await?;
        w.flush().await?;
    }
    Ok(())
}

/// Read a set's members (sets are stored as a JSON array at the key).
fn set_members(st: &SharedShard, key: &str) -> Vec<String> {
    match st.read().unwrap().object(COLL, key) {
        Some(Value::Array(a)) => a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect(),
        _ => Vec::new(),
    }
}

fn s(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

// ---- RESP encoders ----
fn simple(buf: &mut Vec<u8>, s: &str) {
    buf.push(b'+');
    buf.extend_from_slice(s.as_bytes());
    buf.extend_from_slice(b"\r\n");
}
fn err(buf: &mut Vec<u8>, s: &str) {
    buf.push(b'-');
    buf.extend_from_slice(s.as_bytes());
    buf.extend_from_slice(b"\r\n");
}
fn int(buf: &mut Vec<u8>, n: i64) {
    buf.push(b':');
    buf.extend_from_slice(n.to_string().as_bytes());
    buf.extend_from_slice(b"\r\n");
}
fn bulk(buf: &mut Vec<u8>, data: Option<&[u8]>) {
    match data {
        Some(d) => {
            buf.push(b'$');
            buf.extend_from_slice(d.len().to_string().as_bytes());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(d);
            buf.extend_from_slice(b"\r\n");
        }
        None => buf.extend_from_slice(b"$-1\r\n"),
    }
}
/// Encode a stored value as a bulk string: JSON strings as their raw text, anything else as JSON.
fn bulk_value(buf: &mut Vec<u8>, v: Option<&Value>) {
    match v {
        None => bulk(buf, None),
        Some(Value::String(s)) => bulk(buf, Some(s.as_bytes())),
        Some(other) => bulk(buf, Some(serde_json::to_vec(other).unwrap_or_default().as_slice())),
    }
}
fn array_header(buf: &mut Vec<u8>, n: usize) {
    buf.push(b'*');
    buf.extend_from_slice(n.to_string().as_bytes());
    buf.extend_from_slice(b"\r\n");
}

// ---- RESP request parser ----
async fn read_line<R: AsyncBufReadExt + Unpin>(r: &mut R) -> std::io::Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    if r.read_until(b'\n', &mut line).await? == 0 {
        return Ok(None);
    }
    while matches!(line.last(), Some(b'\n') | Some(b'\r')) {
        line.pop();
    }
    Ok(Some(line))
}

/// Parse one command: a RESP array of bulk strings, or a whitespace-split inline command.
async fn read_command<R: AsyncBufReadExt + AsyncReadExt + Unpin>(r: &mut R) -> std::io::Result<Option<Vec<Vec<u8>>>> {
    let line = match read_line(r).await? {
        Some(l) => l,
        None => return Ok(None),
    };
    if line.is_empty() {
        return Ok(Some(vec![]));
    }
    if line[0] == b'*' {
        let n: usize = s(&line[1..]).parse().unwrap_or(0);
        let mut args = Vec::with_capacity(n);
        for _ in 0..n {
            let hdr = match read_line(r).await? {
                Some(h) if !h.is_empty() && h[0] == b'$' => h,
                _ => return Ok(None),
            };
            let len: usize = s(&hdr[1..]).parse().unwrap_or(0);
            let mut data = vec![0u8; len];
            r.read_exact(&mut data).await?;
            let mut crlf = [0u8; 2];
            r.read_exact(&mut crlf).await?;
            args.push(data);
        }
        Ok(Some(args))
    } else {
        Ok(Some(line.split(|b| b.is_ascii_whitespace()).filter(|p| !p.is_empty()).map(<[u8]>::to_vec).collect()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::shared;
    use crate::shard::Shard;
    use crate::store::MemoryStore;

    async fn send(s: &mut TcpStream, cmd: &[u8]) -> Vec<u8> {
        s.write_all(cmd).await.unwrap();
        s.flush().await.unwrap();
        let mut buf = vec![0u8; 4096];
        let n = s.read(&mut buf).await.unwrap();
        buf.truncate(n);
        buf
    }

    #[tokio::test]
    async fn resp_basic_commands() {
        let state: SharedShard = shared(Shard::new("s0", MemoryStore::new()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { serve(listener, state, None).await });

        let mut c = TcpStream::connect(addr).await.unwrap();
        assert_eq!(send(&mut c, b"*1\r\n$4\r\nPING\r\n").await, b"+PONG\r\n");
        // SET k v -> +OK ; GET k -> $1\r\nv\r\n
        assert_eq!(send(&mut c, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n").await, b"+OK\r\n");
        assert_eq!(send(&mut c, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").await, b"$5\r\nhello\r\n");
        // GET missing -> nil
        assert_eq!(send(&mut c, b"*2\r\n$3\r\nGET\r\n$2\r\nno\r\n").await, b"$-1\r\n");
        // INCR counter twice
        assert_eq!(send(&mut c, b"*2\r\n$4\r\nINCR\r\n$1\r\nn\r\n").await, b":1\r\n");
        assert_eq!(send(&mut c, b"*3\r\n$6\r\nINCRBY\r\n$1\r\nn\r\n$2\r\n10\r\n").await, b":11\r\n");
        // SADD + SMEMBERS
        assert_eq!(send(&mut c, b"*4\r\n$4\r\nSADD\r\n$1\r\ns\r\n$1\r\na\r\n$1\r\nb\r\n").await, b":2\r\n");
        assert_eq!(send(&mut c, b"*2\r\n$5\r\nSCARD\r\n$1\r\ns\r\n").await, b":2\r\n");
        // DEL
        assert_eq!(send(&mut c, b"*2\r\n$3\r\nDEL\r\n$1\r\nk\r\n").await, b":1\r\n");
        assert_eq!(send(&mut c, b"*2\r\n$6\r\nEXISTS\r\n$1\r\nk\r\n").await, b":0\r\n");
        // inline command works too
        assert_eq!(send(&mut c, b"PING\r\n").await, b"+PONG\r\n");
    }

    #[tokio::test]
    async fn resp_auth_gate() {
        let state: SharedShard = shared(Shard::new("s0", MemoryStore::new()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { serve(listener, state, Some("sekret".into())).await });

        let mut c = TcpStream::connect(addr).await.unwrap();
        // command before AUTH -> NOAUTH
        let r = send(&mut c, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").await;
        assert!(r.starts_with(b"-NOAUTH"), "got {:?}", String::from_utf8_lossy(&r));
        // wrong then right
        assert!(send(&mut c, b"*2\r\n$4\r\nAUTH\r\n$3\r\nbad\r\n").await.starts_with(b"-WRONGPASS"));
        assert_eq!(send(&mut c, b"*2\r\n$4\r\nAUTH\r\n$6\r\nsekret\r\n").await, b"+OK\r\n");
        assert_eq!(send(&mut c, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n").await, b"+OK\r\n");
    }
}
