package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
)

const jAgentHostPort = "0.0.0.0:6831"

var (
	order = binary.LittleEndian

	reg = regexp.MustCompile(`^.+ (\w+):.+tid = \[ (\[0\] = \d{1,3}(?:, \[\d{1,2}\] = \d{1,3}){31})`)
)

const debug = false

func main() {
	tracer := makeTracer()

	// note: this span should not show up in the final jaeger outputs
	rootSpan := tracer.StartSpan("kernel_root")

	// using parameters `babeltrace --clock-date --clock-gmt --no-delta`
	if debug {
		input := []string{
			`[2018-05-16 01:51:29.858503019] kernel-tracing-ubuntu18 kmem_cache_free: { cpu_id = 4 }, { pid = 3197, tid = [ [0] = 124, [1] = 228, [2] = 28, [3] = 56, [4] = 30, [5] = 168, [6] = 11, [7] = 0, [8] = 124, [9] = 228, [10] = 28, [11] = 56, [12] = 30, [13] = 168, [14] = 11, [15] = 0, [16] = 25, [17] = 111, [18] = 110, [19] = 151, [20] = 169, [21] = 184, [22] = 57, [23] = 235, [24] = 125, [25] = 12, [26] = 0, [27] = 0, [28] = 0, [29] = 0, [30] = 0, [31] = 0 ] }, { call_site = 0xFFFFFFFF8F23EDF0, ptr = 0xFFFF8FEFEA57D000 }`,
			`[2018-05-16 01:51:29.858505538] kernel-tracing-ubuntu18 syscall_exit_newfstat: { cpu_id = 4 }, { pid = 3197, tid = [ [0] = 124, [1] = 228, [2] = 28, [3] = 56, [4] = 30, [5] = 168, [6] = 11, [7] = 0, [8] = 124, [9] = 228, [10] = 28, [11] = 56, [12] = 30, [13] = 168, [14] = 11, [15] = 0, [16] = 227, [17] = 213, [18] = 154, [19] = 97, [20] = 114, [21] = 148, [22] = 101, [23] = 214, [24] = 125, [25] = 12, [26] = 0, [27] = 0, [28] = 0, [29] = 0, [30] = 0, [31] = 0 ] }, { ret = 0, statbuf = 140734798894416 }`,
			`[2018-05-16 01:51:29.858514497] kernel-tracing-ubuntu18 syscall_entry_write: { cpu_id = 4 }, { pid = 3197, tid = [ [0] = 124, [1] = 228, [2] = 28, [3] = 56, [4] = 30, [5] = 168, [6] = 11, [7] = 0, [8] = 124, [9] = 228, [10] = 28, [11] = 56, [12] = 30, [13] = 168, [14] = 11, [15] = 0, [16] = 91, [17] = 86, [18] = 156, [19] = 50, [20] = 12, [21] = 204, [22] = 116, [23] = 51, [24] = 125, [25] = 12, [26] = 0, [27] = 0, [28] = 0, [29] = 0, [30] = 0, [31] = 0 ] }, { fd = 16, buf = 94348888381472, count = 16 }`,
		}

		for _, l := range input {
			process(rootSpan, l)
		}
	} else {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			process(rootSpan, scanner.Text())
		}

		if scanner.Err() != nil {
			// handle error.
		}
	}

	rootSpan.Finish()
}

type threadRunning struct {
	span          *jaeger.Span
	traceID       uint64
	parentID      uint64
	spanID        uint64
	operationName string
	logs          []opentracing.LogRecord
}

var threads = make(map[uint16]*threadRunning)

func process(rootSpan opentracing.Span, line string) {
	if !debug {
		defer func() {
			if r := recover(); r != nil {
				fmt.Println(r, "panic in processTrace:", line)
			}
		}()
	}
	r := processTrace(rootSpan, line)
	fmt.Print(r)
}

func processTrace(rootSpan opentracing.Span, line string) string {
	operationName, curTime, arr := parse(line)
	//fmt.Println(operationName, curTime)

	traceID := order.Uint64(arr[0:8])
	parentID := order.Uint64(arr[8:16])
	spanID := order.Uint64(arr[16:24])
	tid := order.Uint16(arr[24:26])
	//fmt.Println(traceID, parentID, spanID, tid)

	if traceID == 0 || parentID == 0 || spanID == 0 {
		//fmt.Println("dropping")
		return "_"
	}

	//fmt.Println(operationName)

	if strings.HasPrefix(operationName, "syscall") {
		if strings.HasPrefix(operationName, "syscall_entry") {
			operationName = "syscall" + strings.TrimPrefix(operationName, "syscall_entry")

			span := rootSpan.Tracer().StartSpan(
				operationName,
				opentracing.StartTime(curTime)).(*jaeger.Span)

			//fmt.Println(span.Context())
			setContext(span, traceID, spanID, parentID)

			// prints trace, span, parent
			//fmt.Println(span.Context())

			threads[tid] = &threadRunning{
				span:          span,
				traceID:       traceID,
				parentID:      parentID,
				spanID:        spanID,
				operationName: operationName,
				logs: []opentracing.LogRecord{
					{
						Timestamp: curTime,
						Fields:    []log.Field{log.String("entry_raw", line), strTime(curTime)},
					},
				},
			}
		} else if strings.HasPrefix(operationName, "syscall_exit") {
			operationName = "syscall" + strings.TrimPrefix(operationName, "syscall_exit")

			// get thread_running
			thr := threads[tid]
			if thr == nil {
				return "x"
			}

			if thr.operationName != operationName || thr.traceID != traceID || thr.parentID != parentID {
				threads[tid] = nil
				return "x"
			}

			thr.logs = append(thr.logs, opentracing.LogRecord{
				Timestamp: curTime,
				Fields:    []log.Field{log.String("exit_raw", line), strTime(curTime)},
			})

			thr.span.FinishWithOptions(opentracing.FinishOptions{
				FinishTime: curTime,
				LogRecords: thr.logs,
			})

			threads[tid] = nil
		} else {
			// drop
			return "s"
		}
		return "."
	} else {
		// kernel tracepoint event

		thr := threads[tid]
		if thr == nil {
			return "x"
		}

		if thr.traceID != traceID || thr.parentID != parentID || thr.spanID != spanID {
			// ignore events without matching trace/parent IDs
			return "x"
		}

		thr.logs = append(thr.logs, opentracing.LogRecord{
			Timestamp: curTime,
			Fields:    []log.Field{log.String(operationName, line), strTime(curTime)},
		})
		return "k"
	}
}

func parse(line string) (operationName string, tm time.Time, arr []byte) {
	//fmt.Println(line)
	//defer fmt.Println()

	timeBreak := strings.SplitN(strings.TrimPrefix(line, "["), "] ", 2)

	timeStr := timeBreak[0]
	lg := timeBreak[1]

	//fmt.Println(lg)
	lineMatch := reg.FindStringSubmatch(lg)
	//fmt.Println(lineMatch)
	// [0] -> original line
	// [1] -> name
	// [2] -> tid array

	operationName = lineMatch[1]

	strArr := strings.Split(lineMatch[2], ", ")
	for _, a := range strArr {
		num := inty(strings.Split(a, " = ")[1])
		arr = append(arr, byte(num))
	}
	//fmt.Println(arr)

	//fmt.Println(line)

	layout := "2006-01-02 15:04:05.999999999"
	//fmt.Println(timeMatch)
	tm, err := time.Parse(layout, timeStr)
	if err != nil {
		panic(err)
	}
	//fmt.Println(curTime)

	return
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

func strTime(t time.Time) log.Field {
	return log.String("timestamp", fmt.Sprint(t.UnixNano()))
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
		Tags: []opentracing.Tag{
			{
				Key:   "ip",
				Value: "127.0.0.1",
			},
		},
	}

	tracer, _, err := cfg.NewTracer()
	if err != nil {
		fmt.Print("cannot initialize Jaeger Tracer", err)
		os.Exit(1)
	}

	return tracer
}
