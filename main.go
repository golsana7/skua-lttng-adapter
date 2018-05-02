package main

import (
	"fmt"
	"os"
	"reflect"
	"time"
	"unsafe"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
)

const jAgentHostPort = "0.0.0.0:6831"

func main() {
	input := []string{
		`[22:31:49.236500519] (+0.000021655) voxel syscall_entry_write: { cpu_id = 12 }, { pid = 25831, tid = [ [0] = 0, [1] = 0, [2] = 0, [3] = 0, [4] = 0, [5] = 0, [6] = 0, [7] = 0, [8] = 0, [9] = 0, [10] = 0, [11] = 0, [12] = 0, [13] = 0, [14] = 0, [15] = 0, [16] = 0, [17] = 0, [18] = 0, [19] = 0, [20] = 0, [21] = 0, [22] = 0, [23] = 0, [24] = 231, [25] = 100, [26] = 0, [27] = 0, [28] = 0, [29] = 0, [30] = 0, [31] = 0 ] }, { fd = 1, buf = 94637671103600, count = 469 }`,
		`[22:31:49.236514251] (+0.000013732) voxel syscall_exit_write: { cpu_id = 12 }, { pid = 25831, tid = [ [0] = 0, [1] = 0, [2] = 0, [3] = 0, [4] = 0, [5] = 0, [6] = 0, [7] = 0, [8] = 0, [9] = 0, [10] = 0, [11] = 0, [12] = 0, [13] = 0, [14] = 0, [15] = 0, [16] = 0, [17] = 0, [18] = 0, [19] = 0, [20] = 0, [21] = 0, [22] = 0, [23] = 0, [24] = 231, [25] = 100, [26] = 0, [27] = 0, [28] = 0, [29] = 0, [30] = 0, [31] = 0 ] }, { ret = 469 }`,
		`[22:31:49.236524347] (+0.000010096) voxel syscall_exit_select: { cpu_id = 15 }, { pid = 25753, tid = [ [0] = 0, [1] = 0, [2] = 0, [3] = 0, [4] = 0, [5] = 0, [6] = 0, [7] = 0, [8] = 0, [9] = 0, [10] = 0, [11] = 0, [12] = 0, [13] = 0, [14] = 0, [15] = 0, [16] = 0, [17] = 0, [18] = 0, [19] = 0, [20] = 0, [21] = 0, [22] = 0, [23] = 0, [24] = 153, [25] = 100, [26] = 0, [27] = 0, [28] = 0, [29] = 0, [30] = 0, [31] = 0 ] }, { ret = 1, overflow = 0, tvp = 0, _readfds_length = 2, readfds = [ [0] = 0x0, [1] = 0x8 ], _writefds_length = 2, writefds = [ [0] = 0x0, [1] = 0x0 ], _exceptfds_length = 0, exceptfds = [ ] }`,
	}

	tracer := makeTracer()

	// note: this span should not show up in the final jaeger outputs
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

	spanID := uint64(13123123123123)
	traceID := uint64(3452345234523452345)
	parentID := uint64(6789679867896789678)

	span := rootSpan.Tracer().StartSpan(
		operationName,
		opentracing.ChildOf(rootSpan.Context()),
		opentracing.StartTime(startTime)).(*jaeger.Span)

	//fmt.Println(span.Context())
	setContext(span, traceID, spanID, parentID)

	// prints trace, span, parent
	fmt.Println(span.Context())

	span.FinishWithOptions(opentracing.FinishOptions{
		FinishTime: endTime,
		LogRecords: []opentracing.LogRecord{
			{
				Timestamp: endTime,
				Fields:    []log.Field{log.String("raw", line)},
			},
		},
	})
}

func setContext(os *jaeger.Span, trace, span, parent uint64) {
	ros := reflect.ValueOf(os)

	s := (*jaeger.SpanContext)(unsafe.Pointer(ros.Elem().FieldByName("context").UnsafeAddr()))
	rs := reflect.ValueOf(s)

	traceID := (*jaeger.TraceID)(unsafe.Pointer(rs.Elem().FieldByName("traceID").UnsafeAddr()))
	traceID.Low = trace

	spanID := (*uint64)(unsafe.Pointer(rs.Elem().FieldByName("spanID").UnsafeAddr()))
	*spanID = span

	parentID := (*uint64)(unsafe.Pointer(rs.Elem().FieldByName("parentID").UnsafeAddr()))
	*parentID = parent
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
		os.Exit(1)
	}

	return tracer
}
