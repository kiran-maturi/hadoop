/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.tracing;

import io.opentelemetry.context.Context;
import io.opentelemetry.exporter.logging.LoggingSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.OpenTelemetrySdkAutoConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * No-Op Tracer (for now) to remove HTrace without changing too many files.
 */
public class Tracer {
  public static final Logger LOG = LoggerFactory.getLogger(Tracer.class.getName());
  // Singleton
  private static Tracer globalTracer = null;
  private static String OTEL_METRICS_ENV_VARIABLE = "otel.metrics.exporter";
  io.opentelemetry.api.trace.Tracer OTelTracer;
  private final NullTraceScope nullTraceScope;
  private final String name;

  public final static String SPAN_RECEIVER_CLASSES_KEY =
      "span.receiver.classes";

  private Tracer(String name, io.opentelemetry.api.trace.Tracer tracer) {
    this.name = name;
    OTelTracer = tracer;
    nullTraceScope = NullTraceScope.INSTANCE;
  }

  // Keeping this function at the moment for HTrace compatiblity,
  // in fact all threads share a single global tracer for OpenTracing.
  public static Tracer curThreadTracer() {
    return globalTracer;
  }

  /***
   * Return active span.
   * @return org.apache.hadoop.tracing.Span
   */
  public static Span getCurrentSpan() {
    io.opentelemetry.api.trace.Span span = io.opentelemetry.api.trace.Span.current();
    LOG.info("Current span: " + span.toString() + " is valid: " + span.getSpanContext().isValid());
    return span.getSpanContext().isValid()? new Span(span): null;
  }

  public TraceScope newScope(String description) {
    Span span = new Span(OTelTracer.spanBuilder(description).startSpan());
    LOG.info("New span created for the desc: " + description + " span is null: " + String.valueOf(span == null));
    return new TraceScope(span);
  }

  public Span newSpan(String description, SpanContext spanCtx) {
    io.opentelemetry.api.trace.Span parentSpan = io.opentelemetry.api.trace.Span.wrap(spanCtx.getSpanContext());
    io.opentelemetry.api.trace.Span span = OTelTracer.spanBuilder(description).setParent(Context.current().with(parentSpan)).startSpan();
    return new Span(span);
  }

  public TraceScope newScope(String description, SpanContext spanCtx) {
    return new TraceScope(newSpan(description, spanCtx));
  }

  public TraceScope newScope(String description, SpanContext spanCtx,
      boolean finishSpanOnClose) {
    return nullTraceScope;
  }

  public TraceScope activateSpan(Span span) {
    return nullTraceScope;
  }

  public void close() {
  }

  public String getName() {
    return name;
  }

  public static class Builder {
    static Tracer globalTracer;
    private String name;

    public Builder(final String name) {
      this.name = name;
    }

    public Builder conf(TraceConfiguration conf) {
      return this;
    }

    public Tracer build() {
      if (globalTracer == null) {
        LOG.info("Tracer created:");
        //TODO: Move this to config
        Properties properties = System.getProperties();
        properties.setProperty(OTEL_METRICS_ENV_VARIABLE, "none");
        properties.setProperty("otel.traces.exporter", "jaeger");
        properties.setProperty("otel.exporter.jaeger.service.name", name);

        LoggingSpanExporter loggingSpanExporter = new LoggingSpanExporter();
        //TODO: Check if nothing is configured it should return no op tracer
        OpenTelemetrySdk sdk = OpenTelemetrySdkAutoConfiguration.initialize();
        io.opentelemetry.api.trace.Tracer tracer = sdk.getTracer(name);
        globalTracer = new Tracer(name, tracer);
        Tracer.globalTracer = globalTracer;
      }
      return globalTracer;
    }
  }
}
