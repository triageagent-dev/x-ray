package observability

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
)

func TestBearerToken(t *testing.T) {
	var b BearerToken
	if md, _ := b.GetRequestMetadata(context.Background()); len(md) != 0 {
		t.Fatalf("no token set: want no metadata, got %v", md)
	}
	b.Set(" abc\n")
	md, err := b.GetRequestMetadata(context.Background())
	if err != nil || md["authorization"] != "Bearer abc" {
		t.Fatalf("got %v, %v", md, err)
	}
	b.Set("def")
	if md, _ := b.GetRequestMetadata(context.Background()); md["authorization"] != "Bearer def" {
		t.Fatalf("token did not follow Set: %v", md)
	}
	if b.RequireTransportSecurity() {
		t.Fatal("cluster-local collectors speak plaintext gRPC")
	}
}

func TestTraceResourceAttrs_ServiceNamePerInstance(t *testing.T) {
	get := func(attrs []attribute.KeyValue, key string) string {
		for _, a := range attrs {
			if string(a.Key) == key {
				return a.Value.AsString()
			}
		}
		return ""
	}
	def := traceResourceAttrs("", "")
	if got := get(def, "service.name"); got != "xray" {
		t.Errorf("default service.name = %q, want xray", got)
	}
	if got := get(def, "openinference.project.name"); got != "" {
		t.Errorf("no project: got %q", got)
	}
	one := traceResourceAttrs("xray-a", "xray-a")
	if got := get(one, "service.name"); got != "xray-a" {
		t.Errorf("service.name = %q, want xray-a", got)
	}
	if got := get(one, "openinference.project.name"); got != "xray-a" {
		t.Errorf("project = %q, want xray-a", got)
	}
}
