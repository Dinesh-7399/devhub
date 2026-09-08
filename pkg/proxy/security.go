package proxy

import (
	"fmt"
	"net"
	"net/netip"
	"strings"
)

// EgressSecurityPolicy controls destination filtering for egress forward proxy and CONNECT tunnels.
type EgressSecurityPolicy struct {
	BlockMetadata bool
}

// NewEgressSecurityPolicy creates an egress policy with cloud metadata protection enabled by default.
func NewEgressSecurityPolicy(blockMetadata bool) *EgressSecurityPolicy {
	return &EgressSecurityPolicy{
		BlockMetadata: blockMetadata,
	}
}

// Default blocked IPv4 and IPv6 CIDR prefixes for cloud metadata and link-local services.
var (
	ipv4LinkLocal = netip.MustParsePrefix("169.254.0.0/16")
	ipv6LinkLocal = netip.MustParsePrefix("fe80::/10")
)

// ValidateDestination inspects a target "host:port" or "host" and returns an error if it violates security policy.
func (p *EgressSecurityPolicy) ValidateDestination(target string) error {
	if !p.BlockMetadata {
		return nil
	}

	host := target
	if h, _, err := net.SplitHostPort(target); err == nil {
		host = h
	}

	lowerHost := strings.ToLower(strings.TrimSpace(host))

	// 1. Block well-known cloud metadata DNS names
	if lowerHost == "metadata.google.internal" ||
		lowerHost == "instance-data" ||
		lowerHost == "169.254.169.254" ||
		lowerHost == "metadata" {
		return fmt.Errorf("egress destination %q blocked: cloud metadata endpoint", target)
	}

	// 2. Check if host is an IP address
	if addr, err := netip.ParseAddr(lowerHost); err == nil {
		if isMetadataOrLinkLocal(addr) {
			return fmt.Errorf("egress destination %q blocked: link-local/cloud metadata IP", target)
		}
	}

	// 3. Resolve hostnames that might be DNS-rebinding to metadata IPs
	ips, err := net.LookupIP(lowerHost)
	if err == nil {
		for _, ip := range ips {
			if addr, ok := netip.AddrFromSlice(ip); ok {
				if isMetadataOrLinkLocal(addr.Unmap()) {
					return fmt.Errorf("egress destination %q resolved to blocked metadata IP %s", target, addr)
				}
			}
		}
	}

	return nil
}

func isMetadataOrLinkLocal(addr netip.Addr) bool {
	// Check IPv4 Link-Local (169.254.0.0/16)
	if addr.Is4() {
		return ipv4LinkLocal.Contains(addr)
	}

	// Check IPv6 Link-Local (fe80::/10) or AWS EC2 IPv6 metadata (fd00:ec2::254)
	if addr.Is6() {
		if ipv6LinkLocal.Contains(addr) {
			return true
		}
		// Check AWS IPv6 IMDS
		if addr.String() == "fd00:ec2::254" {
			return true
		}
	}

	return false
}
