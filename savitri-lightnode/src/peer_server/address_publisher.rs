use libp2p::multiaddr::Protocol;
use libp2p::Multiaddr;

#[derive(Debug, Clone, Default)]
pub struct AddressPublishOptions {
    pub publish_private_addresses: bool,
    pub rpc_port: Option<u16>,
    pub rpc_bind_addr: Option<String>,
}

fn is_loopback_ip(ip: &std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(ip) => ip.is_loopback(),
        std::net::IpAddr::V6(ip) => ip.is_loopback(),
    }
}

fn is_unspecified_ip(ip: &std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(ip) => ip.is_unspecified(),
        std::net::IpAddr::V6(ip) => ip.is_unspecified(),
    }
}

fn is_private_ip(ip: &std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(ip) => {
            ip.is_private()
                || ip.is_link_local()
                || ip.is_broadcast()
                || matches!(
                    ip.octets(),
                    [192, 0, 2, _] | [198, 51, 100, _] | [203, 0, 113, _]
                )
        }
        std::net::IpAddr::V6(ip) => {
            ip.is_unique_local()
                || ip.is_unicast_link_local()
                || ip.is_unspecified()
                || ip.is_loopback()
                || (ip.segments()[0] == 0x2001 && ip.segments()[1] == 0x0db8)
        }
    }
}

fn extract_ip(addr: &Multiaddr) -> Option<std::net::IpAddr> {
    for protocol in addr.iter() {
        match protocol {
            Protocol::Ip4(ip) => return Some(std::net::IpAddr::V4(ip)),
            Protocol::Ip6(ip) => return Some(std::net::IpAddr::V6(ip)),
            _ => {}
        }
    }
    None
}

fn is_publishable(addr: &Multiaddr, allow_private: bool) -> bool {
    let Some(ip) = extract_ip(addr) else {
        return true;
    };
    if is_unspecified_ip(&ip) {
        return false;
    }
    if allow_private {
        return true;
    }
    !is_loopback_ip(&ip) && !is_private_ip(&ip)
}

fn score(addr: &Multiaddr, allow_private: bool) -> i32 {
    let mut value = 0;
    let ip = extract_ip(addr);
    if ip
        .map(|ip| !is_private_ip(&ip) && !is_loopback_ip(&ip))
        .unwrap_or(false)
    {
        value += 10;
    } else if allow_private {
        value += 2;
    }
    for protocol in addr.iter() {
        match protocol {
            Protocol::QuicV1 => value += 3,
            Protocol::Tcp(_) => value += 2,
            Protocol::Dns(_) | Protocol::Dns4(_) | Protocol::Dns6(_) | Protocol::Dnsaddr(_) => {
                value += 1
            }
            _ => {}
        }
    }
    value
}

pub fn compute_publishable_addresses(
    listen_addrs: &[Multiaddr],
    observed_addr: Option<&Multiaddr>,
    options: &AddressPublishOptions,
) -> Vec<Multiaddr> {
    let mut addresses = Vec::new();
    if let Some(observed_addr) = observed_addr {
        if is_publishable(observed_addr, options.publish_private_addresses) {
            addresses.push(observed_addr.clone());
        }
    }
    for addr in listen_addrs {
        if is_publishable(addr, options.publish_private_addresses) {
            addresses.push(addr.clone());
        }
    }
    addresses.sort_by_key(|addr| std::cmp::Reverse(score(addr, options.publish_private_addresses)));
    addresses.dedup();
    addresses
}

pub fn build_public_rpc_endpoint(
    publish_addrs: &[Multiaddr],
    options: &AddressPublishOptions,
) -> Option<String> {
    let port = options.rpc_port?;
    let bind_addr = options.rpc_bind_addr.as_deref().unwrap_or("127.0.0.1");
    if !options.publish_private_addresses
        && (bind_addr == "127.0.0.1" || bind_addr.eq_ignore_ascii_case("localhost"))
    {
        return None;
    }

    let host = publish_addrs.iter().find_map(|addr| {
        for protocol in addr.iter() {
            match protocol {
                Protocol::Ip4(ip) => return Some(ip.to_string()),
                Protocol::Ip6(ip) => return Some(format!("[{}]", ip)),
                Protocol::Dns(name)
                | Protocol::Dns4(name)
                | Protocol::Dns6(name)
                | Protocol::Dnsaddr(name) => return Some(name.to_string()),
                _ => {}
            }
        }
        None
    })?;
    Some(format!("http://{}:{}", host, port))
}
