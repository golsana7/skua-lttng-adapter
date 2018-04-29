package main

import (
	"time"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go/config"
	"fmt"
	"os"
	"github.com/uber/jaeger-client-go"
)

const jAgentHostPort = "0.0.0.0:6831"

func main() {
	//input := "asdfasdfa"

	tracer := makeTracer()

	jtracer := tracer.(*jaeger.Tracer)

	_ = jtracer
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
