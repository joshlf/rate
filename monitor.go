// Copyright 2014 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Copyright 2014 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rate

import (
	"io"
	"sync/atomic"
	"time"
)

const (
	defaultPeroid = time.Duration(500) * time.Millisecond
)

type Rate struct {
	Total uint64
	Rate  float64
}

// A Monitor monitors the rate at which abstract events
// happen (such as bytes written to an input stream).
// Calling Add(n) signals that n events have happened.
// Every period, the average rate at which events happened
// over the preceding period and the total number of events
// so far are either written to a channel or given as the
// argument to a function.
type Monitor struct {
	f      func(r Rate)
	period time.Duration
	t0     time.Time
	n, nn  uint64
	exit   chan struct{}
}

// MakeMonitor creates a new Monitor which writes
// the rate and total to the returned channel every period.
// If period == 0, the default period of 500ms will
// be used.
func MakeMonitor(period time.Duration) (*Monitor, <-chan Rate) {
	rch := make(chan Rate, 8)
	return MakeMonitorFunc(period, func(r Rate) {
		rch <- r
	}), rch
}

// MakeMonitorFunc creates a new Monitor which calls f
// in a separate goroutine every period. If period
// == 0, the default period of 500ms will be used.
func MakeMonitorFunc(period time.Duration, f func(r Rate)) *Monitor {
	if period == 0 {
		period = defaultPeroid
	}
	ret := &Monitor{
		f:      f,
		period: period,
		exit:   make(chan struct{}, 1),
	}
	go ret.monitor()
	return ret
}

func (m *Monitor) monitor() {
	m.t0 = time.Now()
	for {
		select {
		case <-m.exit:
			return
		default:
			// Use default and sleep instead of
			// a time.After case because extra
			// thread switching under heavy loads
			// makes a big performance difference.
			time.Sleep(m.period)

			// In case we missed an exit command
			// while we were sleeping; this technically
			// wouldn't invalidate the semantics,
			// but it'd still be dumb to unnecessarily
			// be doing stuff hundreds of milliseconds
			// after we were told to stop.
			select {
			case <-m.exit:
				return
			default:
			}

			t1 := time.Now()
			delta := t1.Sub(m.t0)
			m.t0 = t1

			nn := atomic.SwapUint64(&m.nn, 0)
			m.n += nn

			rate := float64(nn) / delta.Seconds()
			m.f(Rate{m.n, rate})
		}
	}
}

func (m *Monitor) Add(n uint64) {
	atomic.AddUint64(&m.nn, n)
}

// Close stops m from monitoring its rate. If m was
// created with MakeMonitor, no more values will be
// written to the channel, and if it was created with
// MakeMonitorFunc, f will not be called again.
func (m *Monitor) Close() {
	// Since m.exit is buffered, the first
	// value will always be sent. This way,
	// subsequent calls to Close will never
	// block.
	select {
	case m.exit <- struct{}{}:
	default:
	}
}

// A MonitorReader wraps an io.Reader and monitors the rate
// at which bytes are read from it. Every period, the average
// rate at which bytes were read over the preceding period
// and the total number of bytes read so far are either written
// to a channel, or passed as the argument to a function.
type MonitorReader struct {
	r   io.Reader
	m   *Monitor
	err error
}

// MakeMonitorReader creates a new MonitorReader which writes
// the rate and total to the returned channel every period.
// If period == 0, the default period of 500ms will be used.
func MakeMonitorReader(r io.Reader, period time.Duration) (*MonitorReader, <-chan Rate) {
	m, rch := MakeMonitor(period)
	return &MonitorReader{r: r, m: m}, rch
}

// MakeMonitorReaderFunc creates a new MonitorReader which calls
// f in a separate goroutine every period. If period == 0, the
// default period of 500ms will be used.
func MakeMonitorReaderFunc(r io.Reader, period time.Duration, f func(r Rate)) *MonitorReader {
	m := MakeMonitorFunc(period, f)
	return &MonitorReader{r: r, m: m}
}

func (m *MonitorReader) Read(p []byte) (n int, err error) {
	if m.err != nil {
		n, err = 0, m.err
		return
	}
	if m.r == nil {
		n, err = 0, io.EOF
		return
	}

	n, err = m.r.Read(p)
	m.m.Add(uint64(n))
	return
}

// Close closes the reader; all subsequent calls to Read
// will return io.EOF or any error previously encountered,
// and the rate will not be reported any more. Additionally,
// if m's underlying Reader implements the io.ReadCloser
// interface, its Close method will be called, and its
// return value will be returned from this method.
//
// If m's underlying writer implements io.ReadCloser,
// but it's undesirable for its Close method to be called,
// wrap it in a ReaderOnly before creating m.
func (m *MonitorReader) Close() error {
	m.m.Close()
	defer func() { m.r = nil }()
	if rc, ok := m.r.(io.ReadCloser); ok {
		return rc.Close()
	}
	return nil
}

// A MonitorWriter wraps an io.Writer and monitors the rate
// at which bytes are written to it, Every period, the average
// rate at which bytes were written over the preceding period
// and the total number of bytes written so far are either
// written to a channel, or passed as the argument to a function.
type MonitorWriter struct {
	w   io.Writer
	m   *Monitor
	err error
}

// MakeMonitorWriter creates a new MonitorWriter which writes
// the rate and total to the returned channel every period.
// If period == 0, the default period of 500ms will be used.
func MakeMonitorWriter(w io.Writer, period time.Duration) (*MonitorWriter, <-chan Rate) {
	m, rch := MakeMonitor(period)
	return &MonitorWriter{w: w, m: m}, rch
}

// MakeMonitorWriterFunc creates a new MonitorWriter which calls
// f in a separate goroutine every period. If period == 0, the
// default period of 500ms will be used.
func MakeMonitorWriterFunc(w io.Writer, period time.Duration, f func(r Rate)) *MonitorWriter {
	m := MakeMonitorFunc(period, f)
	return &MonitorWriter{w: w, m: m}
}

func (m *MonitorWriter) Write(p []byte) (n int, err error) {
	if m.err != nil {
		n, err = 0, m.err
		return
	}
	if m.w == nil {
		n, err = 0, io.EOF
		return
	}

	n, err = m.w.Write(p)
	m.m.Add(uint64(n))
	return
}

// Close closes the writer; all subsequent calls to Write
// will return io.EOF or any error previously encountered,
// and the rate will not be reported any more. Additionally,
// if m's underlying Writer implements the io.WriteCloser
// interface, its Close method will be called, and its
// return value will be returned from this method.
//
// If m's underlying writer implements io.WriteCloser,
// but it's undesirable for its Close method to be called,
// wrap it in a WriterOnly before creating m.
func (m *MonitorWriter) Close() error {
	m.m.Close()
	defer func() { m.w = nil }()
	if wc, ok := m.w.(io.WriteCloser); ok {
		return wc.Close()
	}
	return nil
}

// ReaderOnly allows a type which implements
// more than just the io.Reader interface to
// appear as though it only implements
// io.Reader
type ReaderOnly struct {
	io.Reader
}

func (r ReaderOnly) Read(p []byte) (n int, err error) {
	n, err = r.Reader.Read(p)
	return
}

// WriterOnly allows a type which implements
// more than just the io.Writer interface to
// appear as though it only implements
// io.Writer
type WriterOnly struct {
	io.Writer
}

func (w WriterOnly) Write(p []byte) (n int, err error) {
	n, err = w.Writer.Write(p)
	return
}
