package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A browser sends only the scheme and host as Origin, so the CORS and voice
// allowlists must hold exactly that, whatever path the server is mounted under.
func TestPublicOrigin(t *testing.T) {
	for in, want := range map[string]string{
		"":                       "",
		"https://h.example":      "https://h.example",
		"https://h.example/":     "https://h.example",
		"https://h.example/path": "https://h.example",
		"http://localhost:8080":  "http://localhost:8080",
	} {
		assert.Equal(t, want, publicOrigin(in), in)
	}
}
