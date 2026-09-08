package proxy

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"path/filepath"
	"strings"
)

// Egress security policy modes
const (
	EgressModeAllowAllExceptMetadata = "allow_all_except_metadata" // Default: dev-friendly, blocks cloud metadata
	EgressModeAllowAllExceptPrivate  = "allow_all_except_private"  // Hardened: blocks RFC1918, loopback & metadata
	EgressModeStrict                 = "strict"                    // Strict: only allowed_hosts permitted
)

var (
	// Metadata & Link-Local CIDRs
	cidrIPv4LinkLocal = netip.MustParsePrefix("169.254.0.0/16")
	cidrIPv6LinkLocal = netip.MustParsePrefix("fe80::/10")

	// Private & Loopback CIDRs
	cidrIPv4Loopback = netip.MustParsePrefix("127.0.0.0/8")
	cidrIPv4Private1 = netip.MustParsePrefix("10.0.0.0/8")
	cidrIPv4Private2 = netip.MustParsePrefix("172.16.0.0/12")
	cidrIPv4Private3 = netip.MustParsePrefix("192.168.0.0/16")
	cidrIPv6Loopback = netip.MustParsePrefix("::1/128")
	cidrIPv6ULA      = netip.MustParsePrefix("fc00::/7")
)

// EgressSecurityPolicy controls destination filtering for egress forward proxy and CONNECT tunnels.
type EgressSecurityPolicy struct {
	Mode          string
	BlockMetadata bool
	AllowedHosts  []string
	DeniedCIDRs   []netip.Prefix
	Resolver      *net.Resolver
}

// NewEgressSecurityPolicy creates an egress policy.
func NewEgressSecurityPolicy(mode string, blockMetadata bool) *EgressSecurityPolicy {
	if mode == "" {
		mode = EgressModeAllowAllExceptMetadata
	}
	return &EgressSecurityPolicy{
		Mode:          mode,
		BlockMetadata: blockMetadata,
		AllowedHosts:  nil,
		DeniedCIDRs:   nil,
		Resolver:      nil,
	}
}

// IsHostAllowed checks whether a hostname matches the AllowedHosts whitelist (including wildcards e.g. *.stripe.com).
func (p *EgressSecurityPolicy) IsHostAllowed(host string) bool {
	lowerHost := strings.ToLower(host)
	for _, pattern := range p.AllowedHosts {
		lowerPattern := strings.ToLower(pattern)
		if lowerPattern == lowerHost {
			return true
		}
		if matched, err := filepath.Match(lowerPattern, lowerHost); err == nil && matched {
			return true
		}
	}
	return false
}

func (p *EgressSecurityPolicy) lookupIP(ctx context.Context, host string) ([]net.IP, error) {
	if p.Resolver != nil {
		return p.Resolver.LookupIP(ctx, "ip", host)
	}
	return net.DefaultResolver.LookupIP(ctx, "ip", host)
}

// ResolveAndValidate resolves the target address, validates all resolved IPs against the egress policy,
// and returns the chosen valid IP address along with the destination port.
// Dials must connect directly to this resolved IP to eliminate DNS rebinding TOCTOU vulnerabilities.
func (p *EgressSecurityPolicy) ResolveAndValidate(ctx context.Context, target string) (netip.Addr, string, error) {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		host = target
		port = "80"
	}

	lowerHost := strings.ToLower(strings.TrimSpace(host))

	// 1. Block well-known cloud metadata DNS names
	if p.BlockMetadata || p.Mode != "" {
		if lowerHost == "metadata.google.internal" ||
			lowerHost == "instance-data" ||
			lowerHost == "169.254.169.254" ||
			lowerHost == "metadata" {
			return netip.Addr{}, "", fmt.Errorf("egress destination %q blocked: cloud metadata endpoint", target)
		}
	}

	// 2. Check AllowedHosts whitelist (bypasses private/strict CIDR blocks if explicitly permitted)
	if len(p.AllowedHosts) > 0 && p.IsHostAllowed(lowerHost) {
		// If lowerHost is an IP literal, return it immediately without network DNS query
		if addr, err := netip.ParseAddr(lowerHost); err == nil {
			return addr.Unmap(), port, nil
		}
		// Resolve and return first IP
		ips, err := p.lookupIP(ctx, lowerHost)
		if err != nil {
			return netip.Addr{}, "", fmt.Errorf("failed to resolve allowed host %q: %w", lowerHost, err)
		}
		for _, ip := range ips {
			if addr, ok := netip.AddrFromSlice(ip); ok {
				return addr.Unmap(), port, nil
			}
		}
	}

	// In strict mode, only allowed_hosts are permitted
	if p.Mode == EgressModeStrict {
		return netip.Addr{}, "", fmt.Errorf("egress destination %q blocked: not in allowed_hosts (strict mode)", target)
	}

	// 3. Direct IP target validation
	if addr, err := netip.ParseAddr(lowerHost); err == nil {
		unmapped := addr.Unmap()
		if err := p.validateIP(unmapped); err != nil {
			return netip.Addr{}, "", fmt.Errorf("egress destination %q blocked: %w", target, err)
		}
		return unmapped, port, nil
	}

	// 4. Resolve hostname once and validate all returned A/AAAA records
	ips, err := p.lookupIP(ctx, lowerHost)
	if err != nil {
		return netip.Addr{}, "", fmt.Errorf("failed to resolve egress destination %q: %w", target, err)
	}
	if len(ips) == 0 {
		return netip.Addr{}, "", fmt.Errorf("no IP addresses resolved for egress destination %q", target)
	}

	var validIP netip.Addr
	for _, ip := range ips {
		addr, ok := netip.AddrFromSlice(ip)
		if !ok {
			continue
		}
		unmapped := addr.Unmap()
		if err := p.validateIP(unmapped); err != nil {
			return netip.Addr{}, "", fmt.Errorf("egress destination %q resolved to forbidden IP %s: %w", target, unmapped, err)
		}
		if !validIP.IsValid() {
			validIP = unmapped
		}
	}

	if !validIP.IsValid() {
		return netip.Addr{}, "", fmt.Errorf("no valid IP address found for egress destination %q", target)
	}

	return validIP, port, nil
}

// ValidateDestination provides non-dialing validation for fast rejection.
func (p *EgressSecurityPolicy) ValidateDestination(target string) error {
	_, _, err := p.ResolveAndValidate(context.Background(), target)
	return err
}

// DialValidatedContext resolves the target, validates against the security policy,
// and dials the validated IP directly to prevent DNS rebinding TOCTOU.
func (p *EgressSecurityPolicy) DialValidatedContext(ctx context.Context, dialer *net.Dialer, network, target string) (net.Conn, error) {
	ip, port, err := p.ResolveAndValidate(ctx, target)
	if err != nil {
		return nil, err
	}
	directAddr := net.JoinHostPort(ip.String(), port)
	return dialer.DialContext(ctx, network, directAddr)
}

func (p *EgressSecurityPolicy) validateIP(addr netip.Addr) error {
	// Always block cloud metadata / link-local if BlockMetadata is true
	if p.BlockMetadata {
		if addr.Is4() && cidrIPv4LinkLocal.Contains(addr) {
			return fmt.Errorf("link-local/cloud metadata IPv4 (%s)", addr)
		}
		if addr.Is6() {
			if cidrIPv6LinkLocal.Contains(addr) {
				return fmt.Errorf("link-local IPv6 (%s)", addr)
			}
			if addr.String() == "fd00:ec2::254" {
				return fmt.Errorf("AWS EC2 IPv6 metadata endpoint")
			}
		}
	}

	// Block private subnets if in allow_all_except_private mode
	if p.Mode == EgressModeAllowAllExceptPrivate {
		if addr.IsLoopback() || cidrIPv4Loopback.Contains(addr) || cidrIPv6Loopback.Contains(addr) {
			return fmt.Errorf("loopback destination blocked in allow_all_except_private mode (%s)", addr)
		}
		if addr.Is4() {
			if cidrIPv4Private1.Contains(addr) || cidrIPv4Private2.Contains(addr) || cidrIPv4Private3.Contains(addr) {
				return fmt.Errorf("RFC1918 private IPv4 blocked (%s)", addr)
			}
		}
		if addr.Is6() {
			if cidrIPv6ULA.Contains(addr) {
				return fmt.Errorf("unique local IPv6 blocked (%s)", addr)
			}
		}
	}

	// Check custom DeniedCIDRs
	for _, cidr := range p.DeniedCIDRs {
		if cidr.Contains(addr) {
			return fmt.Errorf("IP %s matches denied CIDR %s", addr, cidr)
		}
	}

	return nil
}

// ParsePrefixes converts slice of CIDR strings into netip.Prefix.
func ParsePrefixes(cidrs []string) []netip.Prefix {
	var out []netip.Prefix
	for _, c := range cidrs {
		if p, err := netip.ParsePrefix(strings.TrimSpace(c)); err == nil {
			out = append(out, p)
		}
	}
	return out
}
