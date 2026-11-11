import { trace } from "@opentelemetry/api";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { NodeTracerProvider } from "@opentelemetry/sdk-trace-node";

const SERVICE = process.env.OTEL_SERVICE_NAME ?? "service";

// The spans go in batches to the address in OTEL_EXPORTER_OTLP_ENDPOINT (Jaeger).
// register() also installs the default propagator, which uses the W3C Trace Context
// "traceparent" header.
const provider = new NodeTracerProvider({
  resource: resourceFromAttributes({ "service.name": SERVICE }),
  spanProcessors: [new BatchSpanProcessor(new OTLPTraceExporter())],
});
provider.register();

export const tracer = trace.getTracer("pedido-certo");

export async function shutdownTelemetry(): Promise<void> {
  await provider.shutdown(); // sends the spans still in the batch
}

// Structured log: one JSON line per event, with the active span's trace_id,
// to go from the log to the trace (and from the trace to the logs) by the same id.
export function log(message: string, fields: Record<string, unknown> = {}): void {
  const span = trace.getActiveSpan();
  console.log(JSON.stringify({
    service: SERVICE,
    message,
    trace_id: span?.spanContext().traceId,
    ...fields,
  }));
}
