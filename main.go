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

	reg = regexp.MustCompile(`^\[(\d\d:\d\d:\d\d\.\d{1,10})\] \(\+\d+.\d+\) .+ (\w+):.+tid = \[ (\[0\] = \d{1,3}(?:, \[\d{1,2}\] = \d{1,3}){31})`)
)

const debug = false

func main() {
	tracer := makeTracer()

	// note: this span should not show up in the final jaeger outputs
	rootSpan := tracer.StartSpan("kernel_root")

	if debug {
		input := []string{
			`[16:16:48.772397108] (+0.000000994) voxel kmem_kfree: { cpu_id = 16 }, { pid = 3494, tid = [ [0] = 113, [1] = 31, [2] = 104, [3] = 211, [4] = 12, [5] = 147, [6] = 45, [7] = 0, [8] = 113, [9] = 31, [10] = 104, [11] = 211, [12] = 12, [13] = 147, [14] = 45, [15] = 0, [16] = 249, [17] = 119, [18] = 75, [19] = 176, [20] = 254, [21] = 27, [22] = 162, [23] = 207, [24] = 166, [25] = 13, [26] = 0, [27] = 0, [28] = 0, [29] = 0, [30] = 0, [31] = 0 ] }, { call_site = 0xFFFFFFFFB72B18C0, ptr = 0xFFFFA1E261CE8A80 }`,
			`[16:16:48.772400477] (+0.000001213) voxel writeback_dirty_inode_start: { cpu_id = 16 }, { pid = 3494, tid = [ [0] = 113, [1] = 31, [2] = 104, [3] = 211, [4] = 12, [5] = 147, [6] = 45, [7] = 0, [8] = 113, [9] = 31, [10] = 104, [11] = 211, [12] = 12, [13] = 147, [14] = 45, [15] = 0, [16] = 249, [17] = 119, [18] = 75, [19] = 176, [20] = 254, [21] = 27, [22] = 162, [23] = 207, [24] = 166, [25] = 13, [26] = 0, [27] = 0, [28] = 0, [29] = 0, [30] = 0, [31] = 0 ] }, { name = "(unknown)", ino = 4026532273, state = 0, flags = 7 }`,
			`[16:16:48.772403141] (+0.000001895) voxel kmem_cache_free: { cpu_id = 16 }, { pid = 3494, tid = [ [0] = 113, [1] = 31, [2] = 104, [3] = 211, [4] = 12, [5] = 147, [6] = 45, [7] = 0, [8] = 113, [9] = 31, [10] = 104, [11] = 211, [12] = 12, [13] = 147, [14] = 45, [15] = 0, [16] = 249, [17] = 119, [18] = 75, [19] = 176, [20] = 254, [21] = 27, [22] = 162, [23] = 207, [24] = 166, [25] = 13, [26] = 0, [27] = 0, [28] = 0, [29] = 0, [30] = 0, [31] = 0 ] }, { call_site = 0xFFFFFFFFB723EDF0, ptr = 0xFFFFA1E26A675000 }`,
			`[16:16:48.772408379] (+0.000003779) voxel syscall_entry_newfstat: { cpu_id = 16 }, { pid = 3494, tid = [ [0] = 113, [1] = 31, [2] = 104, [3] = 211, [4] = 12, [5] = 147, [6] = 45, [7] = 0, [8] = 113, [9] = 31, [10] = 104, [11] = 211, [12] = 12, [13] = 147, [14] = 45, [15] = 0, [16] = 145, [17] = 155, [18] = 174, [19] = 57, [20] = 0, [21] = 4, [22] = 10, [23] = 251, [24] = 166, [25] = 13, [26] = 0, [27] = 0, [28] = 0, [29] = 0, [30] = 0, [31] = 0 ] }, { fd = 16 }`,
			`[16:16:48.772410359] (+0.000001980) voxel syscall_exit_newfstat: { cpu_id = 16 }, { pid = 3494, tid = [ [0] = 113, [1] = 31, [2] = 104, [3] = 211, [4] = 12, [5] = 147, [6] = 45, [7] = 0, [8] = 113, [9] = 31, [10] = 104, [11] = 211, [12] = 12, [13] = 147, [14] = 45, [15] = 0, [16] = 145, [17] = 155, [18] = 174, [19] = 57, [20] = 0, [21] = 4, [22] = 10, [23] = 251, [24] = 166, [25] = 13, [26] = 0, [27] = 0, [28] = 0, [29] = 0, [30] = 0, [31] = 0 ] }, { ret = 0, statbuf = 140729043239456 }`,
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
	span      *jaeger.Span
	traceID uint64
	parentID uint64
	operationName string
	logs []opentracing.LogRecord
}

var threads = make(map[uint16]*threadRunning)

func process(rootSpan opentracing.Span, line string) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println(r, "panic in processTrace:", line)
		}
	}()
	processTrace(rootSpan, line)
}

func processTrace(rootSpan opentracing.Span, line string) {
	//fmt.Println(line)
	//defer fmt.Println()

	lineMatch := reg.FindStringSubmatch(line)
	//fmt.Println(lineMatch)
	// [0] -> original line
	// [1] -> time
	// [2] -> name
	// [3] -> tid array

	strArr := strings.Split(lineMatch[3], ", ")
	var arr []byte
	for _, a := range strArr {
		num := inty(strings.Split(a, " = ")[1])
		arr = append(arr, byte(num))
	}
	//fmt.Println(arr)

	traceID := order.Uint64(arr[0:8])
	parentID := order.Uint64(arr[8:16])
	spanID := order.Uint64(arr[16:24])
	tid := order.Uint16(arr[24:26])
	//fmt.Println(traceID, parentID, spanID, tid)

	if traceID == 0 || parentID == 0 {
		//fmt.Println("dropping")
		return
	}

	//fmt.Println(line)

	timeStr := lineMatch[1]
	//fmt.Println(timeStr)
	tb1 := strings.Split(timeStr, ".")
	tb0 := strings.Split(tb1[0], ":")

	now := time.Now()
	//fmt.Println(timeMatch)
	curTime := time.Date(now.Year(), now.Month(), now.Day(), inty(tb0[0]), inty(tb0[1]), inty(tb0[2]), inty(tb1[1]), now.Location())
	//fmt.Println(curTime)

	operationName := lineMatch[2]
	//fmt.Println(operationName)

	fmt.Print(".")

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
				operationName: operationName,
				logs: []opentracing.LogRecord{
					{
						Timestamp: curTime,
						Fields:    []log.Field{log.String("entry_raw", line)},
					},
				},
			}
		} else if strings.HasPrefix(operationName, "syscall_exit") {
			operationName = "syscall" + strings.TrimPrefix(operationName, "syscall_exit")

			// get thread_running
			thr := threads[tid]
			if thr == nil {
				return
			}

			if thr.operationName != operationName || thr.traceID != traceID || thr.parentID != parentID {
				threads[tid] = nil
				return
			}

			thr.logs = append(thr.logs, opentracing.LogRecord{
				Timestamp: curTime,
				Fields:    []log.Field{log.String("exit_raw", line)},
			})

			thr.span.FinishWithOptions(opentracing.FinishOptions{
				FinishTime: curTime,
				LogRecords: thr.logs,
			})

			threads[tid] = nil
		} else {
			// drop
			fmt.Print("s")
		}
	} else {
		// kernel event

		thr := threads[tid]
		if thr == nil {
			return
		}

		if thr.traceID != traceID || thr.parentID != parentID {
			// ignore events without matching trace/parent IDs
			return
		}

		thr.logs = append(thr.logs, opentracing.LogRecord{
			Timestamp: curTime,
			Fields: []log.Field{log.String("event", line)},
		})
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
