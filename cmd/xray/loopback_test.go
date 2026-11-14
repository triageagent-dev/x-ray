package main

import (
	"strings"
	"testing"
)

func TestIsLoopbackAddr(t *testing.T) {
	cases := map[string]bool{
		"127.0.0.1:8080": true,
		"[::1]:8080":     true,
		"localhost:8080": true,
		":8080":          false,
		"0.0.0.0:8080":   false,
		"[::]:80":        false,
		"10.0.0.5:8080":  false,
		"nonsense":       false,
	}
	for addr, want := range cases {
		if got := isLoopbackAddr(addr); got != want {
			t.Errorf("isLoopbackAddr(%q) = %v, want %v", addr, got, want)
		}
	}
}

// The static-client flags are read only under --mcp-oauth, so either one without it
// must stop the server instead of being ignored.
func TestCheckStaticClientFlags(t *testing.T) {
	cases := []struct {
		name     string
		oauth    bool
		id, uris string
		wantErr  bool
	}{
		{"neither flag, oauth off", false, "", "", false},
		{"neither flag, oauth on", true, "", "", false},
		{"both flags, oauth on", true, "connector-1", "https://connector.example/cb", false},
		{"id only, oauth off", false, "connector-1", "", true},
		{"redirects only, oauth off", false, "", "https://connector.example/cb", true},
		{"both flags, oauth off", false, "connector-1", "https://connector.example/cb", true},
	}
	for _, c := range cases {
		err := checkStaticClientFlags(c.oauth, c.id, c.uris)
		if (err != nil) != c.wantErr {
			t.Errorf("%s: err = %v, wantErr %v", c.name, err, c.wantErr)
		}
		if err != nil && !strings.Contains(err.Error(), "--mcp-oauth") {
			t.Errorf("%s: error %q does not name --mcp-oauth", c.name, err)
		}
	}
}
