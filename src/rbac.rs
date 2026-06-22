// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! Role-based access control: per-token, per-collection permissions for the HTTP API.
//!
//! A [`Policy`] maps bearer tokens to a [`Principal`] holding one or more [`Grant`]s. Each grant
//! scopes a set of operations ([read / write / admin]) to a collection (or `*` for all). The HTTP
//! middleware ([`crate::net::require_policy`]) authenticates the token, classifies the request into
//! an [`Op`] + target collection, and authorizes against the principal's grants — bearer auth grown
//! into an authz model. The simple single-token [`crate::net::require_bearer`] remains for the basic
//! case; RBAC is opt-in.
//!
//! Config string format (one source of truth, easy to put in a flag or secret):
//!   `token:coll:perms;token:coll:perms;...`  where `coll` is a name or `*`, and `perms` is any of
//!   `r` (read), `w` (write), `a` (admin). Repeat a token to give it several grants. Example:
//!   `root:*:rwa;app:kv:rw;reporting:*:r`

use std::collections::HashMap;

use anyhow::{bail, Result};

/// The kind of access a request needs. Derived from the route, not the HTTP method (several POST
/// routes — mget, scanBuckets, export — are reads).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Read,
    Write,
    Admin,
}

/// One permission: a set of ops on a collection (`coll == "*"` matches any collection, and is the
/// only kind of grant that authorizes collection-less / cluster-level requests).
#[derive(Debug, Clone)]
pub struct Grant {
    pub coll: String,
    pub read: bool,
    pub write: bool,
    pub admin: bool,
}

impl Grant {
    fn allows(&self, coll: Option<&str>, op: Op) -> bool {
        let coll_ok = match (self.coll.as_str(), coll) {
            ("*", _) => true,            // wildcard grant matches any collection AND cluster ops
            (_, Some(c)) => self.coll == c,
            (_, None) => false,          // a non-wildcard grant can't authorize a cluster-level op
        };
        coll_ok
            && match op {
                Op::Read => self.read,
                Op::Write => self.write,
                Op::Admin => self.admin,
            }
    }
}

/// An authenticated identity and what it may do.
#[derive(Debug, Clone, Default)]
pub struct Principal {
    pub name: String,
    pub grants: Vec<Grant>,
}

impl Principal {
    pub fn allows(&self, coll: Option<&str>, op: Op) -> bool {
        self.grants.iter().any(|g| g.allows(coll, op))
    }
}

/// Bearer-token → [`Principal`] policy.
#[derive(Debug, Clone, Default)]
pub struct Policy {
    tokens: HashMap<String, Principal>,
}

impl Policy {
    /// Parse the config string (see the module docs). Tokens may not contain `:` or `;`.
    pub fn parse(spec: &str) -> Result<Policy> {
        let mut tokens: HashMap<String, Principal> = HashMap::new();
        for entry in spec.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            let parts: Vec<&str> = entry.split(':').collect();
            if parts.len() != 3 {
                bail!("bad policy entry '{entry}' (want token:coll:perms)");
            }
            let (token, coll, perms) = (parts[0].trim(), parts[1].trim(), parts[2].trim());
            if token.is_empty() || coll.is_empty() {
                bail!("bad policy entry '{entry}': empty token or coll");
            }
            let mut grant = Grant { coll: coll.to_string(), read: false, write: false, admin: false };
            for ch in perms.chars() {
                match ch {
                    'r' => grant.read = true,
                    'w' => grant.write = true,
                    'a' => grant.admin = true,
                    other => bail!("bad permission '{other}' in '{entry}' (want r/w/a)"),
                }
            }
            let p = tokens.entry(token.to_string()).or_insert_with(|| Principal { name: token.to_string(), grants: vec![] });
            p.grants.push(grant);
        }
        if tokens.is_empty() {
            bail!("empty policy");
        }
        Ok(Policy { tokens })
    }

    pub fn principal(&self, token: &str) -> Option<&Principal> {
        self.tokens.get(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_authorize() {
        let p = Policy::parse("root:*:rwa; app:kv:rw ; ro:*:r").unwrap();

        let root = p.principal("root").unwrap();
        assert!(root.allows(Some("kv"), Op::Write));
        assert!(root.allows(None, Op::Admin)); // wildcard authorizes cluster ops

        let app = p.principal("app").unwrap();
        assert!(app.allows(Some("kv"), Op::Read));
        assert!(app.allows(Some("kv"), Op::Write));
        assert!(!app.allows(Some("kv"), Op::Admin), "app has no admin");
        assert!(!app.allows(Some("other"), Op::Read), "app is scoped to kv");
        assert!(!app.allows(None, Op::Read), "scoped grant can't authorize cluster ops");

        let ro = p.principal("ro").unwrap();
        assert!(ro.allows(Some("anything"), Op::Read));
        assert!(!ro.allows(Some("kv"), Op::Write), "read-only");

        assert!(p.principal("unknown").is_none());
    }

    #[test]
    fn multiple_grants_per_token() {
        let p = Policy::parse("svc:kv:rw;svc:logs:r").unwrap();
        let svc = p.principal("svc").unwrap();
        assert!(svc.allows(Some("kv"), Op::Write));
        assert!(svc.allows(Some("logs"), Op::Read));
        assert!(!svc.allows(Some("logs"), Op::Write), "logs is read-only for svc");
    }

    #[test]
    fn rejects_malformed() {
        assert!(Policy::parse("nope").is_err());
        assert!(Policy::parse("t:kv:x").is_err());
        assert!(Policy::parse("").is_err());
    }
}
