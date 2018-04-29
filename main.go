package main

import (
	"time"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	"github.com/uber/jaeger-client-go/config"
	"fmt"
	"os"
	"github.com/uber/jaeger-client-go"
)

const jAgentHostPort = "0.0.0.0:6831"

func main() {
	input := []string{"asdfasdfa", "adfadf"}

	tracer := makeTracer()

	//jtracer := tracer.(*jaeger.Tracer)
	//_ = jtracer

	rootSpan := tracer.StartSpan("kernel_root")

	for _, line := range input {
		processTrace(rootSpan, line)
	}
}

func processTrace(rootSpan opentracing.Span, line string) {
	// TODO: parse data from line
	operationName := "syscall_write"
	startTime := time.Now()
	endTime := startTime.Add(2 * time.Millisecond)

	span := rootSpan.Tracer().StartSpan(
		operationName,
		opentracing.ChildOf(rootSpan.Context()),
		opentracing.StartTime(startTime))

	spanContext := span.Context().(jaeger.SpanContext)
	// TODO use reflection to modify span context
	_ = spanContext

	span.FinishWithOptions(opentracing.FinishOptions{
		FinishTime: endTime,
		LogRecords: []opentracing.LogRecord{
			{Timestamp: endTime, Fields: []log.Field{log.String("raw", line)}},
		},
	})
}

func makeTracer() opentracing.Tracer {
	cfg := config.Configuration{
		ServiceName: "kernel",
		Sampler: &config.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
		Reporter: &config.ReporterConfig{
			LogSpans:            false,
			BufferFlushInterval: 1 * time.Second,
			LocalAgentHostPort:  jAgentHostPort,
		},
	}

	tracer, _, err := cfg.NewTracer()
	if err != nil {
		fmt.Print("cannot initialize Jaeger Tracer", err)
		os.Exit(1);
	}

	return tracer
}
