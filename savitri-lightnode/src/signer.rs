#![allow(dead_code)]

use anyhow::{Context, Result};
use ed25519_dalek::{SigningKey as Keypair, SigningKey};
use rand_core::OsRng;
use std::{fs, path::Path};
use zeroize::Zeroize;

pub fn load_or_generate_ed25519(path: &Path) -> Result<Keypair> {
    if path.exists() {
        // SECURITY: Check file permissions on Unix (should be 0600 — owner read/write only)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = fs::metadata(path)?;
            let mode = metadata.permissions().mode() & 0o777;
            if mode & 0o077 != 0 {
                tracing::warn!(
                    path = %path.display(),
                    mode = format!("{:o}", mode),
                    "Private key file has excessive permissions (expected 0600). Fixing..."
                );
                fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
            }
        }

        let bytes = fs::read(path)?;
        // Accept 32-byte (secret), 64-byte (secret+public), and 68-byte (protobuf) key files
        let mut secret_bytes: [u8; 32] = match bytes.len() {
            32 => bytes
                .as_slice()
                .try_into()
                .context("invalid secret key length")?,
            64 => bytes[..32]
                .try_into()
                .context("invalid secret key length")?,
            68 => {
                // Extract 32-byte Ed25519 secret from 68-byte protobuf identity
                // libp2p protobuf: [headers][32-byte public][32-byte secret]
                if bytes.len() >= 68 {
                    bytes[36..68]
                        .try_into()
                        .context("invalid protobuf secret key length")?
                } else {
                    anyhow::bail!("invalid protobuf key length");
                }
            }
            len => anyhow::bail!(
                "unexpected key length {}; expected 32, 64, or 68 bytes",
                len
            ),
        };
        let secret = SigningKey::from_bytes(&secret_bytes);
        secret_bytes.zeroize();
        return Ok(secret);
    }
    let mut rng = OsRng {};
    let kp = Keypair::generate(&mut rng);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    fs::write(path, kp.to_bytes())?;

    // SECURITY: Set restrictive permissions on newly created key file (Unix only)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }

    Ok(kp)
}
