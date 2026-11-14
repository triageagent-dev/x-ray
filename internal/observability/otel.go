package observability

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"
	"github.com/triageagent-dev/x-ray/v2/internal/version"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	promhttp "github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

var (
	mu          sync.Mutex
	currentStop func(context.Context) error

	// GenAI client metric instruments (https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-metrics/).
	// nil until ConfigureMetrics installs a MeterProvider; record helpers no-op when nil.
	tokenUsageHist  metric.Int64Histogram
	ttftHist        metric.Float64Histogram // time-to-first-token (user speech end → first model audio)
	tpotHist        metric.Float64Histogram // time-per-output-token (output streaming time ÷ output tokens)
	toolDurHist     metric.Float64Histogram // per-tool-call latency, labeled by tool name
	sessionDurHist  metric.Float64Histogram // full voice session duration
	searchDurHist   metric.Float64Histogram // memory search latency, labeled by mode
	searchPhaseHist metric.Float64Histogram // semantic search latency, labeled by phase (embed, index)
)

// BearerToken sends an Authorization: Bearer header on every OTLP export. The
// token can arrive after the exporter starts (it is read from a Secret once the
// cluster cache syncs) and can change while running; until it is set, exports
// go out without the header and the collector decides what to do with them.
type BearerToken struct{ v atomic.Value }

// Set replaces the token.
func (b *BearerToken) Set(token string) { b.v.Store(strings.TrimSpace(token)) }

// GetRequestMetadata implements grpc credentials.PerRPCCredentials.
func (b *BearerToken) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	if t, _ := b.v.Load().(string); t != "" {
		return map[string]string{"authorization": "Bearer " + t}, nil
	}
	return nil, nil
}

// RequireTransportSecurity is false: the collector is usually a cluster-local
// Service reached over plaintext gRPC.
func (b *BearerToken) RequireTransportSecurity() bool { return false }

// Histogram bucket boundaries, in seconds. The OpenTelemetry defaults (0, 5, 10,
// 25 ... 10000) are millisecond-shaped, so every latency here fell into the first
// two buckets and a p95 read as "under 5 s".
var (
	secondsBuckets = []float64{0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 4, 6, 8, 12, 20, 30, 60}
	tokenBuckets   = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1}
	fastBuckets    = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	sessionBuckets = []float64{5, 15, 30, 60, 120, 300, 600, 1200, 1800, 3600}
)

// traceResourceAttrs names the spans' source: service.name (default "xray"; one
// name per instance lets a trace consumer tell instances apart, for example to
// score one and not another) and the openinference project when set.
func traceResourceAttrs(serviceName, projectName string) []attribute.KeyValue {
	name := strings.TrimSpace(serviceName)
	if name == "" {
		name = "xray"
	}
	attrs := []attribute.KeyValue{semconv.ServiceName(name), semconv.ServiceVersion(version.Version)}
	if p := strings.TrimSpace(projectName); p != "" {
		attrs = append(attrs, attribute.String("openinference.project.name", p))
	}
	return attrs
}

// ConfigureOTel sets the global TracerProvider. When endpoint is empty, installs
// a no-op provider (zero overhead). Safe to call multiple times. A non-nil token
// is sent as a bearer header with every export.
func ConfigureOTel(ctx context.Context, endpoint, serviceName, projectName string, token *BearerToken, logger zerolog.Logger) error {
	mu.Lock()
	defer mu.Unlock()

	if strings.TrimSpace(endpoint) == "" {
		if currentStop != nil {
			_ = currentStop(ctx)
			currentStop = nil
		}
		otel.SetTracerProvider(oteltrace.NewNoopTracerProvider())
		return nil
	}

	exp, err := buildExporter(ctx, endpoint, token)
	if err != nil {
		return fmt.Errorf("create otel exporter: %w", err)
	}

	res, err := resource.New(ctx, resource.WithAttributes(traceResourceAttrs(serviceName, projectName)...))
	if err != nil {
		return fmt.Errorf("create otel resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
		sdktrace.WithSpanProcessor(&sessionBaggageProcessor{}),
		sdktrace.WithSpanProcessor(&openInferenceKindNormalizer{}),
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)

	if currentStop != nil {
		_ = currentStop(ctx)
	}
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	currentStop = tp.Shutdown

	logger.Info().
		Str("endpoint", endpoint).
		Str("project", projectName).
		Msg("OpenTelemetry tracing enabled")

	return nil
}

// ConfigureMetrics installs a Prometheus-backed MeterProvider and registers the
// GenAI client metric instruments. It returns an http.Handler that serves the
// metrics in Prometheus text format (mount at /metrics). Metrics are pull-based
// and entirely independent of the OTLP trace export — nothing is pushed to the
// trace backend. Safe to call once at startup.
func ConfigureMetrics(logger zerolog.Logger) (http.Handler, error) {
	mu.Lock()
	defer mu.Unlock()

	registry := prometheus.NewRegistry()
	exporter, err := otelprom.New(otelprom.WithRegisterer(registry))
	if err != nil {
		return nil, fmt.Errorf("create prometheus exporter: %w", err)
	}

	res, err := resource.New(context.Background(), resource.WithAttributes(
		semconv.ServiceName("xray"),
		semconv.ServiceVersion(version.Version),
	))
	if err != nil {
		return nil, fmt.Errorf("create metrics resource: %w", err)
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exporter),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)
	if err := initGenAIInstruments(mp); err != nil {
		return nil, fmt.Errorf("init genai metrics: %w", err)
	}

	logger.Info().Msg("GenAI metrics enabled (Prometheus /metrics)")
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), nil
}

// initGenAIInstruments registers the GenAI client metric instruments per
// https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-metrics/.
func initGenAIInstruments(mp metric.MeterProvider) error {
	meter := mp.Meter("xray.genai")
	var err error
	if tokenUsageHist, err = meter.Int64Histogram(
		"gen_ai.client.token.usage",
		metric.WithUnit("{token}"),
		metric.WithDescription("Number of tokens used in GenAI requests/responses."),
	); err != nil {
		return err
	}
	if ttftHist, err = meter.Float64Histogram(
		"gen_ai.client.operation.time_to_first_token",
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(secondsBuckets...),
		metric.WithDescription("Latency from the end of user input to the first model output chunk."),
	); err != nil {
		return err
	}
	if tpotHist, err = meter.Float64Histogram(
		"gen_ai.client.operation.time_per_output_token",
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(tokenBuckets...),
		metric.WithDescription("Mean output streaming latency per output token within a turn."),
	); err != nil {
		return err
	}
	if toolDurHist, err = meter.Float64Histogram(
		"gen_ai.client.tool.duration",
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(fastBuckets...),
		metric.WithDescription("Per-tool-call execution latency."),
	); err != nil {
		return err
	}
	if sessionDurHist, err = meter.Float64Histogram(
		"gen_ai.client.session.duration",
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(sessionBuckets...),
		metric.WithDescription("Full voice session duration."),
	); err != nil {
		return err
	}
	searchDurHist, err = meter.Float64Histogram(
		"xray.memory.search.duration",
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(fastBuckets...),
		metric.WithDescription("Memory search latency, labeled by search mode."),
	)
	if err != nil {
		return err
	}
	searchPhaseHist, err = meter.Float64Histogram(
		"xray.memory.search.phase.duration",
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(fastBuckets...),
		metric.WithDescription("Semantic search latency by phase: embed (the query embedding call) and index (the in-memory scan). An embedder spike and a scan spike look the same in the total."),
	)
	return err
}

// genAIBaseAttrs are the common GenAI semconv labels.
func genAIBaseAttrs(operation, provider, model string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("gen_ai.operation.name", operation),
		attribute.String("gen_ai.provider.name", provider),
		attribute.String("gen_ai.request.model", model),
	}
}

// RecordGenAITokenUsage records the gen_ai.client.token.usage histogram, split by
// token type ("input"/"output"). No-op when metrics are not configured.
func RecordGenAITokenUsage(ctx context.Context, operation, provider, model string, inputTokens, outputTokens int64) {
	if tokenUsageHist == nil {
		return
	}
	base := genAIBaseAttrs(operation, provider, model)
	if inputTokens > 0 {
		tokenUsageHist.Record(ctx, inputTokens, metric.WithAttributes(append(base, attribute.String("gen_ai.token.type", "input"))...))
	}
	if outputTokens > 0 {
		tokenUsageHist.Record(ctx, outputTokens, metric.WithAttributes(append(base, attribute.String("gen_ai.token.type", "output"))...))
	}
}

// RecordGenAITokenUsageByModality records the gen_ai.client.token.usage histogram
// split by token type ("input"/"output") AND modality ("audio"/"text"/…), so the
// metric reconciles to the per-modality Vertex Live billing SKUs (audio-in vs
// text-in vs audio-out vs text-out). tokenType is "input" or "output"; modality is
// the lower-cased SDK modality string. No-op when metrics are not configured or
// tokens <= 0.
func RecordGenAITokenUsageByModality(ctx context.Context, operation, provider, model, tokenType, modality string, tokens int64) {
	if tokenUsageHist == nil || tokens <= 0 {
		return
	}
	attrs := append(genAIBaseAttrs(operation, provider, model),
		attribute.String("gen_ai.token.type", tokenType),
		attribute.String("gen_ai.modality", modality),
	)
	tokenUsageHist.Record(ctx, tokens, metric.WithAttributes(attrs...))
}

// RecordGenAITimeToFirstToken records time-to-first-token (TTFT): the latency from
// the end of user input to the first model output chunk. No-op when not configured.
func RecordGenAITimeToFirstToken(ctx context.Context, operation, provider, model string, seconds float64) {
	if ttftHist == nil {
		return
	}
	ttftHist.Record(ctx, seconds, metric.WithAttributes(genAIBaseAttrs(operation, provider, model)...))
}

// RecordGenAITimePerOutputToken records TPOT: mean output streaming latency per
// output token within a turn (output-streaming duration ÷ output token count).
// No-op when not configured.
func RecordGenAITimePerOutputToken(ctx context.Context, operation, provider, model string, seconds float64) {
	if tpotHist == nil {
		return
	}
	tpotHist.Record(ctx, seconds, metric.WithAttributes(genAIBaseAttrs(operation, provider, model)...))
}

// RecordGenAIToolDuration records a tool call's execution latency, labeled by tool
// name (gen_ai.tool.name). No-op when not configured.
func RecordGenAIToolDuration(ctx context.Context, toolName string, seconds float64) {
	if toolDurHist == nil {
		return
	}
	toolDurHist.Record(ctx, seconds, metric.WithAttributes(attribute.String("gen_ai.tool.name", toolName)))
}

// RecordGenAISessionDuration records a completed voice session's wall-clock
// duration. No-op when not configured.
func RecordGenAISessionDuration(ctx context.Context, provider, model string, seconds float64) {
	if sessionDurHist == nil {
		return
	}
	sessionDurHist.Record(ctx, seconds, metric.WithAttributes(genAIBaseAttrs("voice_session", provider, model)...))
}

// RecordMemorySearchDuration records a memory search's latency, labeled by mode
// ("lexical" for the no-network fast path, "fused" when semantic search ran).
// No-op when not configured.
func RecordMemorySearchDuration(ctx context.Context, mode string, seconds float64) {
	if searchDurHist == nil {
		return
	}
	searchDurHist.Record(ctx, seconds, metric.WithAttributes(attribute.String("search.mode", mode)))
}

// RecordMemorySearchPhase records one phase of a semantic search ("embed" or
// "index"). No-op when not configured.
func RecordMemorySearchPhase(ctx context.Context, phase string, seconds float64) {
	if searchPhaseHist == nil {
		return
	}
	searchPhaseHist.Record(ctx, seconds, metric.WithAttributes(attribute.String("search.phase", phase)))
}

func buildExporter(ctx context.Context, endpoint string, token *BearerToken) (*otlptrace.Exporter, error) {
	opts := []otlptracegrpc.Option{}
	if token != nil {
		opts = append(opts, otlptracegrpc.WithDialOption(grpc.WithPerRPCCredentials(token)))
	}
	if host, insecure := parseEndpointHost(endpoint); host != "" {
		opts = append(opts, otlptracegrpc.WithEndpoint(host))
		if insecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
	} else {
		opts = append(opts, otlptracegrpc.WithEndpoint(endpoint), otlptracegrpc.WithInsecure())
	}
	return otlptracegrpc.New(ctx, opts...)
}

func parseEndpointHost(endpoint string) (host string, insecure bool) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		return "", false
	}
	return u.Host, strings.EqualFold(u.Scheme, "http")
}

// openInferenceKindNormalizer stamps OpenInference span kinds onto spans that
// don't set the attribute explicitly.
type openInferenceKindNormalizer struct{}

func (n *openInferenceKindNormalizer) OnStart(_ context.Context, s sdktrace.ReadWriteSpan) {
	if hasOpenInferenceKind(s.Attributes()) {
		return
	}
	if kind := inferKind(s.Name(), s.Attributes()); kind != "" {
		s.SetAttributes(attribute.String("openinference.span.kind", kind))
	}
}

func (n *openInferenceKindNormalizer) OnEnd(sdktrace.ReadOnlySpan)      {}
func (n *openInferenceKindNormalizer) Shutdown(context.Context) error   { return nil }
func (n *openInferenceKindNormalizer) ForceFlush(context.Context) error { return nil }

func hasOpenInferenceKind(attrs []attribute.KeyValue) bool {
	for _, kv := range attrs {
		if string(kv.Key) == "openinference.span.kind" && strings.TrimSpace(kv.Value.AsString()) != "" {
			return true
		}
	}
	return false
}

// ContextWithSession is the Go equivalent of Phoenix's setSession() — it stores
// session.id and user.id in baggage so the sessionBaggageProcessor stamps them
// onto every child span automatically. Phoenix indexes sessions on "session.id".
func ContextWithSession(ctx context.Context, sessionID, userID string) context.Context {
	b := baggage.FromContext(ctx)
	if m, err := baggage.NewMember("session.id", sessionID); err == nil {
		b, _ = b.SetMember(m)
	}
	if userID != "" {
		if m, err := baggage.NewMember("user.id", userID); err == nil {
			b, _ = b.SetMember(m)
		}
	}
	return baggage.ContextWithBaggage(ctx, b)
}

// sessionBaggageProcessor copies session/user baggage onto every span.
type sessionBaggageProcessor struct{}

func (p *sessionBaggageProcessor) OnStart(ctx context.Context, s sdktrace.ReadWriteSpan) {
	b := baggage.FromContext(ctx)
	for _, key := range []string{"session.id", "user.id"} {
		if v := b.Member(key).Value(); v != "" {
			s.SetAttributes(attribute.String(key, v))
		}
	}
}
func (p *sessionBaggageProcessor) OnEnd(sdktrace.ReadOnlySpan)      {}
func (p *sessionBaggageProcessor) Shutdown(context.Context) error   { return nil }
func (p *sessionBaggageProcessor) ForceFlush(context.Context) error { return nil }

func inferKind(name string, attrs []attribute.KeyValue) string {
	lower := strings.ToLower(strings.TrimSpace(name))
	switch {
	case strings.HasPrefix(lower, "llm."):
		return "LLM"
	case strings.HasPrefix(lower, "tool."):
		return "TOOL"
	case strings.HasPrefix(lower, "voice."):
		return "CHAIN"
	}
	for _, kv := range attrs {
		if strings.HasPrefix(strings.ToLower(string(kv.Key)), "tool.") {
			return "TOOL"
		}
	}
	return ""
}
