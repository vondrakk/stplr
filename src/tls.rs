// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! In-process TLS for both transports.
//!
//! One `rustls::ServerConfig` (loaded from a PEM cert chain + key) drives the HTTP plane
//! (axum-server) and the binary hot-path (tokio-rustls) — same materials, same handshake. Clients
//! (reqwest, used by the coordinator → shard hop and the smart client) get a [`ClientTls`] that can
//! trust a private cluster CA or, for dev, skip verification. The crypto provider is `ring`
//! throughout (small + portable; aws-lc-rs's generated sources overflow constrained build hosts),
//! installed once at process start by [`init`].

use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};

use anyhow::{bail, Context, Result};
use axum::Router;

/// Install the process-default crypto provider (ring). Idempotent: a second call (e.g. a test that
/// also runs under the binary) is ignored. Call before building any rustls config.
pub fn init() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Server-side TLS materials, shared by the HTTP and binary protocols.
#[derive(Clone)]
pub struct ServerTls {
    pub config: Arc<rustls::ServerConfig>,
}

impl ServerTls {
    /// Load a PEM cert chain (`cert_path`) and private key (`key_path`) into a server config.
    pub fn from_pem(cert_path: &str, key_path: &str) -> Result<ServerTls> {
        init();
        let certs = load_certs(cert_path)?;
        let key = load_key(key_path)?;
        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .context("building TLS server config from cert/key")?;
        Ok(ServerTls { config: Arc::new(config) })
    }

    /// Like [`from_pem`](Self::from_pem) but **requires a client certificate** signed by the CA in
    /// `client_ca_path` (mutual TLS). Used for a dedicated client-facing listener; the handshake is
    /// rejected if the client presents no cert or one not chaining to that CA. Internal
    /// coordinator→shard traffic keeps using the plain server-TLS config, so it's unaffected.
    pub fn from_pem_mtls(cert_path: &str, key_path: &str, client_ca_path: &str) -> Result<ServerTls> {
        init();
        let certs = load_certs(cert_path)?;
        let key = load_key(key_path)?;
        let mut roots = rustls::RootCertStore::empty();
        for c in load_certs(client_ca_path)? {
            roots.add(c).context("adding client CA cert")?;
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .context("building client-certificate verifier")?;
        let config = rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .context("building mTLS server config")?;
        Ok(ServerTls { config: Arc::new(config) })
    }

    /// Acceptor that wraps each accepted raw TCP connection in a TLS session (binary protocol).
    pub fn acceptor(&self) -> tokio_rustls::TlsAcceptor {
        tokio_rustls::TlsAcceptor::from(self.config.clone())
    }

    /// The same config as an axum-server rustls config (HTTP plane).
    pub fn axum_config(&self) -> axum_server::tls_rustls::RustlsConfig {
        axum_server::tls_rustls::RustlsConfig::from_config(self.config.clone())
    }
}

fn load_certs(path: &str) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let mut r = BufReader::new(File::open(path).with_context(|| format!("opening cert {path}"))?);
    let certs: Vec<_> = rustls_pemfile::certs(&mut r)
        .collect::<std::result::Result<_, _>>()
        .with_context(|| format!("reading certs from {path}"))?;
    if certs.is_empty() {
        bail!("no certificates found in {path}");
    }
    Ok(certs)
}

fn load_key(path: &str) -> Result<rustls::pki_types::PrivateKeyDer<'static>> {
    let mut r = BufReader::new(File::open(path).with_context(|| format!("opening key {path}"))?);
    rustls_pemfile::private_key(&mut r)
        .with_context(|| format!("reading private key from {path}"))?
        .with_context(|| format!("no private key found in {path}"))
}

/// Serve an axum `Router` on `addr`, over TLS when `tls` is set, plaintext otherwise. The single
/// HTTP entry point for the shard `app` and the coordinator — TLS is one optional argument.
pub async fn serve_router(addr: SocketAddr, router: Router, tls: Option<ServerTls>) -> std::io::Result<()> {
    match tls {
        Some(t) => axum_server::bind_rustls(addr, t.axum_config()).serve(router.into_make_service()).await,
        None => {
            let listener = tokio::net::TcpListener::bind(addr).await?;
            axum::serve(listener, router).await
        }
    }
}

/// Client-side TLS options for the reqwest-based shard/coordinator clients.
#[derive(Clone, Default)]
pub struct ClientTls {
    /// Path to a PEM CA certificate to trust (a private/self-signed cluster CA).
    pub ca_pem: Option<String>,
    /// Skip certificate verification entirely. DEV ONLY — never in production.
    pub insecure: bool,
}

impl ClientTls {
    /// Apply these options to a reqwest client builder (rustls/ring backend, manual roots).
    pub fn apply(&self, mut b: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
        if let Some(path) = &self.ca_pem {
            if let Ok(pem) = std::fs::read(path) {
                if let Ok(cert) = reqwest::Certificate::from_pem(&pem) {
                    b = b.add_root_certificate(cert);
                }
            }
        }
        if self.insecure {
            b = b.danger_accept_invalid_certs(true);
        }
        b
    }
}

// The cluster-wide client TLS config is process-global: every HttpShardClient / SmartClient the
// node builds (for the coordinator -> shard hop) reads it, so it need not be threaded through every
// constructor and discovery path. Set once at startup; defaults to "no client TLS".
static CLIENT_TLS: OnceLock<ClientTls> = OnceLock::new();

/// Install the process-wide client TLS config (call once at startup if the cluster uses https).
pub fn set_client_tls(c: ClientTls) {
    let _ = CLIENT_TLS.set(c);
}

/// The process-wide client TLS config (default = none).
pub fn client_tls() -> ClientTls {
    CLIENT_TLS.get().cloned().unwrap_or_default()
}

#[cfg(test)]
pub(crate) mod testutil {
    /// Generate a self-signed cert+key for localhost, write them to PEM files in `dir`, and return
    /// `(cert_path, key_path, ca_pem_bytes)`. The leaf is its own CA for a self-signed cert.
    pub fn self_signed(dir: &std::path::Path) -> (String, String, Vec<u8>) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
        let cert_pem = cert.cert.pem();
        let key_pem = cert.key_pair.serialize_pem();
        let cert_path = dir.join("cert.pem");
        let key_path = dir.join("key.pem");
        std::fs::write(&cert_path, &cert_pem).unwrap();
        std::fs::write(&key_path, &key_pem).unwrap();
        (cert_path.to_string_lossy().into(), key_path.to_string_lossy().into(), cert_pem.into_bytes())
    }

    /// Like [`self_signed`] but with BOTH serverAuth + clientAuth EKUs, so the one cert can act as the
    /// server cert, the client CA, and a client identity in an mTLS test. Returns
    /// `(cert_path, key_path, cert_pem, key_pem)`.
    pub fn self_signed_dual(dir: &std::path::Path) -> (String, String, String, String) {
        let mut params = rcgen::CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
        params.extended_key_usages =
            vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth, rcgen::ExtendedKeyUsagePurpose::ClientAuth];
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        let cert_pem = cert.pem();
        let key_pem = key.serialize_pem();
        let cert_path = dir.join("c.pem");
        let key_path = dir.join("k.pem");
        std::fs::write(&cert_path, &cert_pem).unwrap();
        std::fs::write(&key_path, &key_pem).unwrap();
        (cert_path.to_string_lossy().into(), key_path.to_string_lossy().into(), cert_pem, key_pem)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mtls_accepts_valid_client_cert_rejects_none() {
        use crate::net::{app, shared};
        use crate::shard::Shard;
        use crate::store::MemoryStore;
        let dir = std::env::temp_dir().join(format!("stplr-mtls-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (cert, key, cert_pem, key_pem) = testutil::self_signed_dual(&dir);
        // server cert = cert; client CA = the same self-signed cert (so it trusts a client presenting it)
        let mtls = ServerTls::from_pem_mtls(&cert, &key, &cert).unwrap();

        let state = shared(Shard::new("s0", MemoryStore::new()));
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let cfg = mtls.axum_config();
        let router = app(state);
        tokio::spawn(async move {
            let _ = axum_server::from_tcp_rustls(listener, cfg).serve(router.into_make_service()).await;
        });

        let server_ca = reqwest::Certificate::from_pem(cert_pem.as_bytes()).unwrap();
        let url = format!("https://127.0.0.1:{}/health", addr.port());

        // a client presenting a valid cert is accepted
        let id = reqwest::Identity::from_pem(format!("{cert_pem}{key_pem}").as_bytes()).unwrap();
        let with = reqwest::Client::builder().add_root_certificate(server_ca.clone()).identity(id).build().unwrap();
        let mut ok = false;
        for _ in 0..30 {
            if let Ok(r) = with.get(&url).send().await {
                ok = r.status().is_success();
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(ok, "valid client cert accepted over mTLS");

        // a client presenting NO cert is rejected at the handshake (server is confirmed up above)
        let without = reqwest::Client::builder().add_root_certificate(server_ca).build().unwrap();
        assert!(without.get(&url).send().await.is_err(), "no client cert -> rejected by mTLS");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn loads_pem_into_server_config() {
        let dir = std::env::temp_dir().join(format!("stplr-tls-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (cert, key, _ca) = testutil::self_signed(&dir);
        let s = ServerTls::from_pem(&cert, &key).expect("load self-signed");
        let _ = s.acceptor();
        let _ = s.axum_config();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn http_tls_roundtrip() {
        use crate::net::{app, shared};
        use crate::shard::Shard;
        use crate::store::MemoryStore;
        let dir = std::env::temp_dir().join(format!("stplr-http-tls-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (cert, key, ca) = testutil::self_signed(&dir);
        let server_tls = ServerTls::from_pem(&cert, &key).unwrap();

        let _ = ca; // the cert file itself is the CA (self-signed leaf)
        let state = shared(Shard::new("s0", MemoryStore::new()));
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let cfg = server_tls.axum_config();
        let router = app(state);
        tokio::spawn(async move {
            let _ = axum_server::from_tcp_rustls(listener, cfg).serve(router.into_make_service()).await;
        });

        // A reqwest client (ring backend) that trusts the cluster CA via ClientTls.
        let client = ClientTls { ca_pem: Some(cert.clone()), insecure: false }
            .apply(reqwest::Client::builder())
            .build()
            .unwrap();
        // give the server a moment to start listening, then hit /health over https
        let url = format!("https://127.0.0.1:{}/health", addr.port());
        let mut last = None;
        for _ in 0..20 {
            match client.get(&url).send().await {
                Ok(r) => {
                    last = Some(r);
                    break;
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
            }
        }
        assert!(last.expect("https health response").status().is_success(), "health over TLS");
        std::fs::remove_dir_all(&dir).ok();
    }
}
