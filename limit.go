// Copyright 2014 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rate

import (
	"io"
	"math"
	"time"
)

const (
	defaultQuantum = time.Millisecond * time.Duration(100)
)

type either interface {
	io(p []byte) (n int, err error)
}

type eitherReader struct {
	r io.Reader
}

func (e eitherReader) io(p []byte) (int, error) { return e.r.Read(p) }

type eitherWriter struct {
	w io.Writer
}

func (e eitherWriter) io(p []byte) (int, error) { return e.w.Write(p) }

// limit implements the abstract functionality
// common between writers and readers
type limit struct {
	e      either
	writer bool

	bps     uint64 // units per second
	quantum time.Duration
	bpq     int // units per quantum

	t0   time.Time
	left int
}

func newLimit(e either, writer bool, bps uint64, quantum time.Duration) limit {
	if bps == 0 {
		// Short-circuit so we don't divide by 0

		// It only matters that bps and e are set
		// so that Read calls with n > 0 will block
		// forever.
		return limit{e: e, bps: bps}
	}
	ret := limit{
		e:       e,
		writer:  writer,
		bps:     bps,
		quantum: quantum,
		bpq:     (int(bps) * int(quantum)) / int(time.Second),
	}
	if ret.bpq == 0 {
		ret.bpq = 1
		ret.quantum = time.Second / time.Duration(ret.bps)
	}
	return ret
}

func (l *limit) io(p []byte) (n int, err error) {
	if l.e == nil {
		n, err = 0, io.EOF
		return
	}
	if len(p) == 0 {
		return
	}
	if l.bps == 0 {
		time.Sleep(time.Duration(math.MaxInt16))
	}

	if l.left == 0 {
		// If there are no bytes left in this quantum,
		// wait until the next one.

		// If l.t0 is the zero value of time.Time,
		// (indicating that this is the first read)
		// l.t0.Sub(time.Now()) < 0, and time.Sleep
		// will return immediately.
		time.Sleep(l.t0.Sub(time.Now()))
		l.t0 = time.Now().Add(l.quantum)
		l.left = l.bpq
	}

	buf := p
	if len(p) > l.left {
		// fmt.Fprintf(os.Stderr, "%v %v\n", len(p), l.left)
		buf = p[:l.left]
	}
	n, err = l.e.io(buf)
	l.left -= n
	if l.writer && err == nil {
		var ntmp int
		ntmp, err = l.io(p[len(buf):])
		n += ntmp
	}
	return
}

type limitReader struct {
	l limit
}

func (l *limitReader) Read(p []byte) (n int, err error) {
	n, err = l.l.io(p)
	return
}

// NewLimitReader returns a new Reader that reads from
// r at a maximum rate of bps bytes per second. If
// bps == 0, any call to Read with len(p) > 0 will
// sleep forever.
func NewLimitReader(r io.Reader, bps uint64) io.Reader {
	return NewLimitReaderQuantum(r, bps, defaultQuantum)
}

// NewLimitReaderQuantum creates a new Reader that
// reads from r at a maximum rate of bps bytes
// per second. If bps == 0, any call to Write with
// len(p) > 0 will sleep forever.
//
// The returned Reader will attempt to control
// the rate by limiting the number of bytes
// read every quantum to bps * quantum. Smaller
// values of quantum will make the rate more smooth
// so long as calls to Read return quickly
// enough and with enough data. However, if
// quantum is too small, it may slow the rate
// due to the overhead of many small read calls.
// The default value (used by NewLimitReader) is 100ms.
func NewLimitReaderQuantum(r io.Reader, bps uint64, quantum time.Duration) io.Reader {
	return &limitReader{newLimit(eitherReader{r}, false, bps, quantum)}
}

type limitWriter struct {
	l limit
}

func (l *limitWriter) Write(p []byte) (n int, err error) {
	n, err = l.l.io(p)
	return
}

// NewLimitWriter returns a new Writer that writes to w
// at a maximum rate of bps bytes per second. If bps == 0,
// any call to Write with len(p) > 0 will sleep forever.
func NewLimitWriter(w io.Writer, bps uint64) io.Writer {
	return NewLimitWriterQuantum(w, bps, defaultQuantum)
}

// NewLimitWriterQuantum creates a new Writer that
// writes to w at a maximum rate of bps bytes
// per second. If bps == 0, any call to Write with
// len(p) > 0 will sleep forever.
//
// The returned Writer will attempt to control
// the rate by limiting the number of bytes
// written every quantum to bps * quantum
// (if Write is called with len(p) > bpq * quantum,
// it will write bpq * quantum and sleep repeatedly
// until all of the bytes have been written). Smaller
// values of quantum will make the rate more smooth
// so long as calls to Write return quickly
// enough and with enough data. However, if
// quantum is too small, it may slow the rate
// due to the overhead of many small read calls.
// The default value (used by NewLimitWriter) is 100ms.
func NewLimitWriterQuantum(w io.Writer, bps uint64, quantum time.Duration) io.Writer {
	return &limitWriter{newLimit(eitherWriter{w}, true, bps, quantum)}
}
