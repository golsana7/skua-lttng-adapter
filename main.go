package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"bufio"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
)

const jAgentHostPort = "0.0.0.0:6831"

var (
	order = binary.LittleEndian

	reLine = regexp.MustCompile(`^\[(.+)\] \(\+(.+)\) .+ syscall_(.+)_(.+): .+ tid = \[ (\[0\].+\[31\] = \d+) \] .+$`)
	reDate = regexp.MustCompile(`^(\d+):(\d+):(\d+).(\d+)$`)
	reDur  = regexp.MustCompile(`^(\d+).(\d+)$`)
)

const debug = false

func main() {
	tracer := makeTracer()

	// note: this span should not show up in the final jaeger outputs
	rootSpan := tracer.StartSpan("kernel_root")

	if debug {
		input := []string{
			`[22:31:49.236500519] (+0.000021655) voxel syscall_entry_write: { cpu_id = 12 }, { pid = 25831, tid = [ [0] = 0, [1] = 0, [2] = 0, [3] = 0, [4] = 0, [5] = 0, [6] = 0, [7] = 0, [8] = 0, [9] = 0, [10] = 0, [11] = 0, [12] = 0, [13] = 0, [14] = 0, [15] = 0, [16] = 0, [17] = 0, [18] = 0, [19] = 0, [20] = 0, [21] = 0, [22] = 0, [23] = 0, [24] = 231, [25] = 100, [26] = 0, [27] = 0, [28] = 0, [29] = 0, [30] = 0, [31] = 0 ] }, { fd = 1, buf = 94637671103600, count = 469 }`,
			`[22:31:49.236514251] (+0.000013732) voxel syscall_exit_write: { cpu_id = 12 }, { pid = 25831, tid = [ [0] = 0, [1] = 0, [2] = 0, [3] = 0, [4] = 0, [5] = 0, [6] = 0, [7] = 0, [8] = 0, [9] = 0, [10] = 0, [11] = 0, [12] = 0, [13] = 0, [14] = 0, [15] = 0, [16] = 0, [17] = 0, [18] = 0, [19] = 0, [20] = 0, [21] = 0, [22] = 0, [23] = 0, [24] = 231, [25] = 100, [26] = 0, [27] = 0, [28] = 0, [29] = 0, [30] = 0, [31] = 0 ] }, { ret = 469 }`,
			`[22:31:49.236524347] (+0.000010096) voxel syscall_exit_select: { cpu_id = 15 }, { pid = 25753, tid = [ [0] = 0, [1] = 0, [2] = 0, [3] = 0, [4] = 0, [5] = 0, [6] = 0, [7] = 0, [8] = 0, [9] = 0, [10] = 0, [11] = 0, [12] = 0, [13] = 0, [14] = 0, [15] = 0, [16] = 0, [17] = 0, [18] = 0, [19] = 0, [20] = 0, [21] = 0, [22] = 0, [23] = 0, [24] = 153, [25] = 100, [26] = 0, [27] = 0, [28] = 0, [29] = 0, [30] = 0, [31] = 0 ] }, { ret = 1, overflow = 0, tvp = 0, _readfds_length = 2, readfds = [ [0] = 0x0, [1] = 0x8 ], _writefds_length = 2, writefds = [ [0] = 0x0, [1] = 0x0 ], _exceptfds_length = 0, exceptfds = [ ] }`,
		}

		for _, l := range input {
			processTrace(rootSpan, l)
		}
	} else {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			processTrace(rootSpan, scanner.Text())
		}

		if scanner.Err() != nil {
			// handle error.
		}
	}

	rootSpan.Finish()
}

type threadRunning struct {
	span      *jaeger.Span
	startTime time.Time
	entryLog  string
}

var threads = make(map[uint16]*threadRunning)

func processTrace(rootSpan opentracing.Span, line string) {
	//fmt.Println(line)
	//defer fmt.Println()

	lineMatch := reLine.FindStringSubmatch(line)[1:]
	//fmt.Println(lineMatch)

	strArr := strings.Split(lineMatch[4], ", ")
	var arr []byte
	for _, a := range strArr {
		num := inty(strings.Split(a, " = ")[1])
		arr = append(arr, byte(num))
	}
	//fmt.Println(arr)

	spanID := order.Uint64(arr[0:8])
	traceID := order.Uint64(arr[8:16])
	parentID := order.Uint64(arr[16:24])
	tid := order.Uint16(arr[24:26])

	if traceID == 0 || parentID == 0 {
		//fmt.Println("dropping")
		return
	}

	fmt.Println(line)

	timeMatch := reDate.FindStringSubmatch(lineMatch[0])[1:]
	durMatch := reDur.FindStringSubmatch(lineMatch[1])[1:]

	now := time.Now()
	startTime := time.Date(now.Year(), now.Month(), now.Day(), inty(timeMatch[0]), inty(timeMatch[1]), inty(timeMatch[2]), inty(timeMatch[3]), now.Location())
	fmt.Println(startTime)

	duration := time.Duration(inty(durMatch[0]))*time.Second + time.Duration(inty(durMatch[1]))*time.Nanosecond
	fmt.Println(duration)
	endTime := startTime.Add(duration)

	fmt.Println(spanID, traceID, parentID, tid)

	if lineMatch[2] == "entry" {
		operationName := fmt.Sprintf("syscall_%s", lineMatch[3])
		fmt.Println(operationName)

		span := rootSpan.Tracer().StartSpan(
			operationName,
			opentracing.StartTime(startTime)).(*jaeger.Span)

		//fmt.Println(span.Context())
		setContext(span, traceID, spanID, parentID)

		// prints trace, span, parent
		//fmt.Println(span.Context())

		threads[tid] = &threadRunning{
			span:      span,
			startTime: startTime,
			entryLog:  line,
		}
	} else if lineMatch[2] == "exit" {
		// get thread_running
		thr := threads[tid]
		if thr == nil {
			return
		}

		thr.span.FinishWithOptions(opentracing.FinishOptions{
			FinishTime: endTime,
			LogRecords: []opentracing.LogRecord{
				{
					Timestamp: startTime,
					Fields:    []log.Field{log.String("entry_raw", thr.entryLog)},
				},
				{
					Timestamp: endTime,
					Fields:    []log.Field{log.String("exit_raw", line)},
				},
			},
		})

		threads[tid] = nil
	} else {
		// drop
	}

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

func inty(l string) int {
	x, _ := strconv.Atoi(l)
	return x
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
