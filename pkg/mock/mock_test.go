package mock

import (
	"testing"
)

func TestMockManager_SetAndGet(t *testing.T) {
	mgr := NewManager()

	mgr.SetRule(MockRule{
		Prefix:     "/api/payment",
		Enabled:    true,
		Mode:       ModeAlways,
		StatusCode: 201,
		Body:       `{"status":"paid","id":"ch_123"}`,
	})

	mgr.SetRule(MockRule{
		Prefix:     "/api/payment/refund",
		Enabled:    true,
		Mode:       ModeAlways,
		StatusCode: 200,
		Body:       `{"status":"refunded"}`,
	})

	// Match longest prefix
	rule, found := mgr.GetRule("/api/payment/refund/item")
	if !found {
		t.Fatalf("expected rule to be found")
	}
	if rule.Prefix != "/api/payment/refund" {
		t.Errorf("expected longest match /api/payment/refund, got %s", rule.Prefix)
	}
	if rule.StatusCode != 200 {
		t.Errorf("expected status 200, got %d", rule.StatusCode)
	}

	// Match shorter prefix
	rule2, found := mgr.GetRule("/api/payment/checkout")
	if !found {
		t.Fatalf("expected rule2 to be found")
	}
	if rule2.Prefix != "/api/payment" {
		t.Errorf("expected /api/payment, got %s", rule2.Prefix)
	}
	if rule2.StatusCode != 201 {
		t.Errorf("expected status 201, got %d", rule2.StatusCode)
	}

	// Unmatched
	_, found = mgr.GetRule("/api/unmocked")
	if found {
		t.Errorf("expected no match for unmocked path")
	}
}

func TestMockManager_Toggle(t *testing.T) {
	mgr := NewManager()

	mgr.SetRule(MockRule{
		Prefix:  "/api/auth",
		Enabled: true,
	})

	rule, found := mgr.GetRule("/api/auth/me")
	if !found || !rule.Enabled {
		t.Fatalf("expected enabled mock rule")
	}

	mgr.ToggleRule("/api/auth", false)

	_, found = mgr.GetRule("/api/auth/me")
	if found {
		t.Fatalf("expected disabled rule to not match")
	}
}
