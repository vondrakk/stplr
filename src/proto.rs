// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! Binary shard protocol — a lean framed wire for the hot path, replacing the HTTP/JSON envelope.
//!
//! The benchmark showed the GET bottleneck is transport, not the shard lock: ~70 µs/op, almost all
//! of it HTTP parse + JSON encode/decode, with the LMDB read itself a rounding error. This is the
//! fix: persistent TCP connections, length-prefixed frames, no per-request HTTP machinery and no
//! JSON envelope. Values cross the wire as opaque bytes (the store's serialized value), so the only
//! serialization left is the value itself.
//!
//! Frame format. Request: `op:u8` then, per op, zero or more byte-strings each `len:u32-be` + bytes.
//! Response: `status:u8` (0 = ok / present, 1 = absent for GET, 2 = bool-false) then an optional
//! `len:u32-be` + payload. One request, one response, pipelined on the same connection.

use std::io;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

use crate::net::SharedShard;

pub const OP_GET: u8 = 1; // coll, key            -> present? + value-bytes
pub const OP_SET: u8 = 2; // coll, key, val       -> ok
pub const OP_SADD: u8 = 3; // coll, key, member   -> bool
pub const OP_SREM: u8 = 4; // coll, key, member   -> bool
pub const OP_DEL: u8 = 5; // coll, key            -> ok
pub const OP_PING: u8 = 6; // -                    -> ok
pub const OP_AUTH: u8 = 7; // token                -> ok (must be the first frame when a token is set)

const ST_OK: u8 = 0; // ok / present (payload follows for GET)
const ST_ABSENT: u8 = 1; // GET miss / bool-false
const ST_TRUE: u8 = 2; // bool-true

// Group-commit writer: OP_SET writes funnel through one mpsc channel into a single writer task
// that coalesces however many are queued into ONE LMDB transaction (one commit for the batch).
// Writes pay the txn/commit cost once per batch instead of once each — the write-throughput win.
const BATCH_MAX: usize = 512;
const CHAN_CAP: usize = 2048;

struct WriteReq {
    coll: String,
    key: String,
    val: Value,
    reply: oneshot::Sender<()>,
}

async fn writer_loop(mut rx: mpsc::Receiver<WriteReq>, st: SharedShard) {
    let mut batch: Vec<(String, String, Value)> = Vec::with_capacity(BATCH_MAX);
    let mut replies: Vec<oneshot::Sender<()>> = Vec::with_capacity(BATCH_MAX);
    loop {
        // Block for at least one write...
        let first = match rx.recv().await {
            Some(r) => r,
            None => break, // all senders dropped
        };
        batch.push((first.coll, first.key, first.val));
        replies.push(first.reply);
        // ...then sweep up everything else already queued (no waiting), up to BATCH_MAX.
        while batch.len() < BATCH_MAX {
            match rx.try_recv() {
                Ok(r) => {
                    batch.push((r.coll, r.key, r.val));
                    replies.push(r.reply);
                }
                Err(_) => break,
            }
        }
        // One write lock, one txn, one commit for the whole batch.
        crate::metrics::SET.fetch_add(replies.len() as u64, std::sync::atomic::Ordering::Relaxed);
        st.write().unwrap().write_batch(std::mem::take(&mut batch));
        batch = Vec::with_capacity(BATCH_MAX);
        for r in replies.drain(..) {
            let _ = r.send(());
        }
    }
}

async fn read_field<R: AsyncReadExt + Unpin>(r: &mut R) -> io::Result<Vec<u8>> {
    let n = r.read_u32().await? as usize;
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}

fn as_str(b: &[u8]) -> &str {
    std::str::from_utf8(b).unwrap_or("")
}

/// Accept loop: one task per connection. Cheap to keep open; clients pool them. When `token` is set,
/// a connection must authenticate with `OP_AUTH` (matching token) before any other op is served.
pub async fn serve(listener: TcpListener, state: SharedShard, token: Option<String>) {
    serve_tls(listener, state, token, None).await
}

/// Like [`serve`], but when `tls` is set every accepted connection is wrapped in a TLS session
/// before the (transport-agnostic) handler runs. `set_nodelay` is applied to the raw socket first,
/// so the latency tuning survives the TLS wrap.
pub async fn serve_tls(
    listener: TcpListener,
    state: SharedShard,
    token: Option<String>,
    tls: Option<crate::tls::ServerTls>,
) {
    // One group-commit writer task for the whole shard; connections send SET writes to it.
    let (wtx, wrx) = mpsc::channel::<WriteReq>(CHAN_CAP);
    tokio::spawn(writer_loop(wrx, state.clone()));
    let token = std::sync::Arc::new(token);
    let acceptor = tls.map(|t| t.acceptor());
    loop {
        let (sock, _) = match listener.accept().await {
            Ok(x) => x,
            Err(_) => continue,
        };
        sock.set_nodelay(true).ok(); // latency over throughput-batching for small ops
        let st = state.clone();
        let wtx = wtx.clone();
        let token = token.clone();
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            match acceptor {
                Some(acc) => match acc.accept(sock).await {
                    Ok(stream) => {
                        let _ = handle(stream, st, wtx, token).await;
                    }
                    Err(_) => {} // failed TLS handshake: drop the connection
                },
                None => {
                    let _ = handle(sock, st, wtx, token).await;
                }
            }
        });
    }
}

async fn handle<S>(
    sock: S,
    st: SharedShard,
    wtx: mpsc::Sender<WriteReq>,
    token: std::sync::Arc<Option<String>>,
) -> io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    // `set_nodelay` is applied to the raw TcpStream in the accept loop (before any TLS wrap), so
    // this handler is transport-agnostic: a plain TcpStream or a tokio-rustls TlsStream both fit.
    let (rd, wr) = tokio::io::split(sock);
    let mut r = BufReader::new(rd);
    let mut w = BufWriter::new(wr);
    let mut authed = token.is_none(); // no token configured => open
    loop {
        // EOF on the opcode read = client closed the connection.
        let op = match r.read_u8().await {
            Ok(o) => o,
            Err(_) => break,
        };
        // Until authenticated, only OP_AUTH is accepted.
        if op == OP_AUTH {
            let presented = read_field(&mut r).await?;
            authed = token.as_deref().map(|t| presented == t.as_bytes()).unwrap_or(true);
            w.write_u8(if authed { ST_OK } else { ST_ABSENT }).await?;
            w.flush().await?;
            if authed {
                continue;
            }
            break; // bad token: drop the connection
        }
        if !authed {
            break; // unauthenticated op: drop the connection
        }
        match op {
            OP_GET => {
                let coll = read_field(&mut r).await?;
                let key = read_field(&mut r).await?;
                crate::metrics::inc(&crate::metrics::GET);
                // Guard taken and dropped synchronously — never held across an await.
                let val = st.read().unwrap().object(as_str(&coll), as_str(&key));
                match val {
                    Some(v) => {
                        let bytes = serde_json::to_vec(&v).unwrap_or_default();
                        w.write_u8(ST_OK).await?;
                        w.write_u32(bytes.len() as u32).await?;
                        w.write_all(&bytes).await?;
                    }
                    None => w.write_u8(ST_ABSENT).await?,
                }
            }
            OP_SET => {
                let coll = read_field(&mut r).await?;
                let key = read_field(&mut r).await?;
                let val = read_field(&mut r).await?;
                let v = serde_json::from_slice::<Value>(&val)
                    .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&val).into_owned()));
                // Funnel into the group-commit writer and wait for the batch's commit (preserves
                // read-after-write: the value is durable+visible before we ack).
                let (rtx, rrx) = oneshot::channel();
                let req = WriteReq {
                    coll: String::from_utf8_lossy(&coll).into_owned(),
                    key: String::from_utf8_lossy(&key).into_owned(),
                    val: v,
                    reply: rtx,
                };
                if wtx.send(req).await.is_err() {
                    break; // writer task gone
                }
                let _ = rrx.await;
                w.write_u8(ST_OK).await?;
            }
            OP_SADD => {
                let coll = read_field(&mut r).await?;
                let key = read_field(&mut r).await?;
                let member = read_field(&mut r).await?;
                crate::metrics::inc(&crate::metrics::SADD);
                let added = st.write().unwrap().set_add(as_str(&coll), as_str(&key), as_str(&member));
                w.write_u8(if added { ST_TRUE } else { ST_ABSENT }).await?;
            }
            OP_SREM => {
                let coll = read_field(&mut r).await?;
                let key = read_field(&mut r).await?;
                let member = read_field(&mut r).await?;
                crate::metrics::inc(&crate::metrics::SREM);
                let removed = st.write().unwrap().set_remove(as_str(&coll), as_str(&key), as_str(&member));
                w.write_u8(if removed { ST_TRUE } else { ST_ABSENT }).await?;
            }
            OP_DEL => {
                let coll = read_field(&mut r).await?;
                let key = read_field(&mut r).await?;
                crate::metrics::inc(&crate::metrics::DEL);
                st.write().unwrap().delete_object(as_str(&coll), as_str(&key));
                w.write_u8(ST_OK).await?;
            }
            OP_PING => w.write_u8(ST_OK).await?,
            _ => break, // unknown opcode: drop the connection
        }
        w.flush().await?;
    }
    Ok(())
}

/// Bind and serve the binary protocol (used by stplrd and tests). `token` mirrors the HTTP bearer.
pub async fn serve_addr(addr: std::net::SocketAddr, state: SharedShard, token: Option<String>) -> io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    serve(listener, state, token).await;
    Ok(())
}

/// Bind and serve the binary protocol over TLS (when `tls` is `Some`) or plaintext (`None`).
pub async fn serve_addr_tls(
    addr: std::net::SocketAddr,
    state: SharedShard,
    token: Option<String>,
    tls: Option<crate::tls::ServerTls>,
) -> io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    serve_tls(listener, state, token, tls).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::shared;
    use crate::shard::Shard;
    use crate::store::MemoryStore;
    use tokio::net::TcpStream;

    async fn req(stream: &mut TcpStream, frame: &[u8]) -> Vec<u8> {
        stream.write_all(frame).await.unwrap();
        stream.flush().await.unwrap();
        let st = stream.read_u8().await.unwrap();
        if st == ST_OK {
            // could be a bare ok or a GET payload; peek by trying to read a len (GET only).
        }
        vec![st]
    }

    fn field(b: &str) -> Vec<u8> {
        let mut v = (b.len() as u32).to_be_bytes().to_vec();
        v.extend_from_slice(b.as_bytes());
        v
    }

    #[tokio::test]
    async fn binary_set_get_roundtrip() {
        let state: SharedShard = shared(Shard::new("s0", MemoryStore::new()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { serve(listener, state, None).await });

        let mut s = TcpStream::connect(addr).await.unwrap();
        // SET kv/k1 = "hello"
        let mut set = vec![OP_SET];
        set.extend(field("kv"));
        set.extend(field("k1"));
        set.extend(field("\"hello\""));
        let r = req(&mut s, &set).await;
        assert_eq!(r[0], ST_OK);

        // GET kv/k1 -> present + "hello"
        let mut get = vec![OP_GET];
        get.extend(field("kv"));
        get.extend(field("k1"));
        s.write_all(&get).await.unwrap();
        s.flush().await.unwrap();
        assert_eq!(s.read_u8().await.unwrap(), ST_OK, "present");
        let n = s.read_u32().await.unwrap() as usize;
        let mut buf = vec![0u8; n];
        s.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, b"\"hello\"");

        // GET miss
        let mut miss = vec![OP_GET];
        miss.extend(field("kv"));
        miss.extend(field("nope"));
        s.write_all(&miss).await.unwrap();
        s.flush().await.unwrap();
        assert_eq!(s.read_u8().await.unwrap(), ST_ABSENT);
    }

    #[tokio::test]
    async fn binary_tls_roundtrip() {
        crate::tls::init();
        let dir = std::env::temp_dir().join(format!("stplr-proto-tls-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (cert, key, ca) = crate::tls::testutil::self_signed(&dir);
        let server_tls = crate::tls::ServerTls::from_pem(&cert, &key).unwrap();

        let state: SharedShard = shared(Shard::new("s0", MemoryStore::new()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { serve_tls(listener, state, None, Some(server_tls)).await });

        // A TLS client that trusts the self-signed cert.
        let mut roots = rustls::RootCertStore::empty();
        for c in rustls_pemfile::certs(&mut std::io::BufReader::new(ca.as_slice())) {
            roots.add(c.unwrap()).unwrap();
        }
        let cfg = rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(cfg));
        let name = rustls::pki_types::ServerName::try_from("localhost".to_owned()).unwrap();
        let tcp = TcpStream::connect(addr).await.unwrap();
        let mut s = connector.connect(name, tcp).await.expect("TLS handshake");

        // SET then GET, end to end over the encrypted connection.
        let mut set = vec![OP_SET];
        set.extend(field("kv"));
        set.extend(field("k1"));
        set.extend(field("\"hi\""));
        s.write_all(&set).await.unwrap();
        s.flush().await.unwrap();
        assert_eq!(s.read_u8().await.unwrap(), ST_OK, "set ok over TLS");

        let mut get = vec![OP_GET];
        get.extend(field("kv"));
        get.extend(field("k1"));
        s.write_all(&get).await.unwrap();
        s.flush().await.unwrap();
        assert_eq!(s.read_u8().await.unwrap(), ST_OK, "present over TLS");
        let n = s.read_u32().await.unwrap() as usize;
        let mut buf = vec![0u8; n];
        s.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, b"\"hi\"", "value round-trips over TLS");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn binary_auth_handshake() {
        let state: SharedShard = shared(Shard::new("s0", MemoryStore::new()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { serve(listener, state, Some("sekret".to_string())).await });

        // An op before auth is rejected (connection dropped → read fails).
        let mut bad = TcpStream::connect(addr).await.unwrap();
        let mut g = vec![OP_GET];
        g.extend(field("kv"));
        g.extend(field("k"));
        bad.write_all(&g).await.unwrap();
        bad.flush().await.unwrap();
        assert!(bad.read_u8().await.is_err(), "unauthenticated op dropped");

        // Wrong token is rejected.
        let mut w = TcpStream::connect(addr).await.unwrap();
        let mut a = vec![OP_AUTH];
        a.extend(field("nope"));
        w.write_all(&a).await.unwrap();
        w.flush().await.unwrap();
        assert_eq!(w.read_u8().await.unwrap(), ST_ABSENT, "wrong token rejected");

        // Correct token, then ops work.
        let mut s = TcpStream::connect(addr).await.unwrap();
        let mut a = vec![OP_AUTH];
        a.extend(field("sekret"));
        s.write_all(&a).await.unwrap();
        s.flush().await.unwrap();
        assert_eq!(s.read_u8().await.unwrap(), ST_OK, "auth ok");
        let mut set = vec![OP_SET];
        set.extend(field("kv"));
        set.extend(field("k1"));
        set.extend(field("\"v\""));
        s.write_all(&set).await.unwrap();
        s.flush().await.unwrap();
        assert_eq!(s.read_u8().await.unwrap(), ST_OK, "authed op works");
    }
}
