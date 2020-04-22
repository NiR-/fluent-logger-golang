package fluent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/bmizerany/assert"
	"github.com/tinylib/msgp/msgp"
)

const (
	RECV_BUF_LEN = 1024
)

func newTestDialer() *testDialer {
	return &testDialer{
		connCh: make(chan *Conn),
	}
}

type testDialer struct {
	connCh chan *Conn
}

func (d *testDialer) DialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	select {
	case conn := <-d.connCh:
		if conn == nil {
			return nil, errors.New("failed to dial")
		}
		return conn, nil
	case <-ctx.Done():
		return nil, errors.New("failed to dial")
	}
}

type nextWrite struct {
	accept bool
	ack    string
}

func (d *testDialer) waitForNewConn(accept bool) *Conn {
	var conn *Conn
	if accept {
		conn = &Conn{
			nextWriteCh: make(chan nextWrite),
			writesCh:    make(chan []byte),
		}
	}

	d.connCh <- conn
	return conn
}

func assertReceived(t *testing.T, rcv []byte, expected string) {
	if string(rcv) != expected {
		t.Fatalf("got %s, expect %s", string(rcv), expected)
	}
}

// Conn is net.Conn with the parameters to be verified in the test
type Conn struct {
	net.Conn
	buf           []byte
	writeDeadline time.Time
	// nextWriteCh is used by tests to let Conn.Write() know if the next write
	// should succeed or fail.
	nextWriteCh chan nextWrite
	// writesCh is used by Conn.Write() to signal to test cases when a write
	// happened.
	writesCh chan []byte
}

func (c *Conn) waitForNextWrite(accept bool, ack string) []byte {
	c.nextWriteCh <- nextWrite{accept, ack}
	if accept {
		return <-c.writesCh
	}
	return []byte{}
}

func (c *Conn) Read(b []byte) (int, error) {
	copy(b, c.buf)
	return len(c.buf), nil
}

func (c *Conn) Write(b []byte) (int, error) {
	next, ok := nextWrite{true, ""}, true
	if c.nextWriteCh != nil {
		next, ok = <-c.nextWriteCh
	}
	if !next.accept || !ok {
		return 0, errors.New("transient write failure")
	}

	// Write the acknowledgment to c.buf to make it available to subsequent
	// call to Read().
	c.buf = make([]byte, len(next.ack))
	copy(c.buf, next.ack)

	// Write the payload received to writesCh to assert on it.
	if c.writesCh != nil {
		c.writesCh <- b
	}

	return len(b), nil
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.writeDeadline = t
	return nil
}

func (c *Conn) Close() error {
	return nil
}

func Test_New_itShouldUseDefaultConfigValuesIfNoOtherProvided(t *testing.T) {
	f, _ := New(Config{})
	assert.Equal(t, f.Config.FluentPort, defaultPort)
	assert.Equal(t, f.Config.FluentHost, defaultHost)
	assert.Equal(t, f.Config.Timeout, defaultTimeout)
	assert.Equal(t, f.Config.WriteTimeout, defaultWriteTimeout)
	assert.Equal(t, f.Config.BufferLimit, defaultBufferLimit)
	assert.Equal(t, f.Config.FluentNetwork, defaultNetwork)
	assert.Equal(t, f.Config.FluentSocketPath, defaultSocketPath)
}

func Test_New_itShouldUseUnixDomainSocketIfUnixSocketSpecified(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("windows not supported")
	}
	socketFile := "/tmp/fluent-logger-golang.sock"
	network := "unix"
	l, err := net.Listen(network, socketFile)
	if err != nil {
		t.Error(err)
		return
	}
	defer l.Close()

	f, err := New(Config{
		FluentNetwork:    network,
		FluentSocketPath: socketFile})
	if err != nil {
		t.Error(err)
		return
	}
	defer f.Close()
	assert.Equal(t, f.Config.FluentNetwork, network)
	assert.Equal(t, f.Config.FluentSocketPath, socketFile)

	socketFile = "/tmp/fluent-logger-golang-xxx.sock"
	network = "unixxxx"
	fUnknown, err := New(Config{
		FluentNetwork:    network,
		FluentSocketPath: socketFile})
	if _, ok := err.(*ErrUnknownNetwork); !ok {
		t.Errorf("err type: %T", err)
	}
	if err == nil {
		t.Error(err)
		fUnknown.Close()
		return
	}
}

func Test_New_itShouldUseConfigValuesFromArguments(t *testing.T) {
	f, _ := New(Config{FluentPort: 6666, FluentHost: "foobarhost"})
	assert.Equal(t, f.Config.FluentPort, 6666)
	assert.Equal(t, f.Config.FluentHost, "foobarhost")
}

func Test_New_itShouldUseConfigValuesFromMashalAsJSONArgument(t *testing.T) {
	f, _ := New(Config{MarshalAsJSON: true})
	assert.Equal(t, f.Config.MarshalAsJSON, true)
}

func Test_MarshalAsMsgpack(t *testing.T) {
	f := &Fluent{Config: Config{}}

	tag := "tag"
	var data = map[string]string{
		"foo":  "bar",
		"hoge": "hoge"}
	tm := time.Unix(1267867237, 0)
	result, err := f.EncodeData(tag, tm, data)

	if err != nil {
		t.Error(err)
	}
	actual := string(result.data)

	// map entries are disordered in golang
	expected1 := "\x94\xA3tag\xD2K\x92\u001Ee\x82\xA3foo\xA3bar\xA4hoge\xA4hoge\x80"
	expected2 := "\x94\xA3tag\xD2K\x92\u001Ee\x82\xA4hoge\xA4hoge\xA3foo\xA3bar\x80"
	if actual != expected1 && actual != expected2 {
		t.Errorf("got %+v,\n         except %+v\n             or %+v", actual, expected1, expected2)
	}
}

func Test_SubSecondPrecision(t *testing.T) {
	// Setup the test subject
	fluent := &Fluent{
		Config: Config{
			SubSecondPrecision: true,
		},
	}
	fluent.conn = &Conn{}

	// Exercise the test subject
	timestamp := time.Unix(1267867237, 256)
	encodedData, err := fluent.EncodeData("tag", timestamp, map[string]string{
		"foo": "bar",
	})

	// Assert no encoding errors and that the timestamp has been encoded into
	// the message as expected.
	if err != nil {
		t.Error(err)
	}

	// 8 bytes timestamp can be represented using ext 8 or fixext 8
	expected1 := "\x94\xA3tag\xC7\x08\x00K\x92\u001Ee\x00\x00\x01\x00\x81\xA3foo\xA3bar\x80"
	expected2 := "\x94\xa3tag\xD7\x00K\x92\x1Ee\x00\x00\x01\x00\x81\xA3foo\xA3bar\x80"
	actual := string(encodedData.data)
	if actual != expected1 && actual != expected2 {
		t.Errorf("got %+v,\n         except %+v\n             or %+v", actual, expected1, expected2)
	}
}

func Test_MarshalAsJSON(t *testing.T) {
	f := &Fluent{Config: Config{MarshalAsJSON: true}}

	var data = map[string]string{
		"foo":  "bar",
		"hoge": "hoge"}
	tm := time.Unix(1267867237, 0)
	result, err := f.EncodeData("tag", tm, data)

	if err != nil {
		t.Error(err)
	}
	// json.Encode marshals map keys in the order, so this expectation is safe
	expected := `["tag",1267867237,{"foo":"bar","hoge":"hoge"},{}]`
	actual := string(result.data)
	if actual != expected {
		t.Errorf("got %s, except %s", actual, expected)
	}
}

func TestJsonConfig(t *testing.T) {
	b, err := ioutil.ReadFile(`testdata/config.json`)
	if err != nil {
		t.Error(err)
	}
	var got Config
	expect := Config{
		FluentPort:         8888,
		FluentHost:         "localhost",
		FluentNetwork:      "tcp",
		FluentSocketPath:   "/var/tmp/fluent.sock",
		Timeout:            3000,
		WriteTimeout:       6000,
		BufferLimit:        10,
		RetryWait:          5,
		MaxRetry:           3,
		TagPrefix:          "fluent",
		Async:              false,
		ForceStopAsyncSend: false,
		MarshalAsJSON:      true,
	}

	err = json.Unmarshal(b, &got)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(expect, got) {
		t.Errorf("got %v, except %v", got, expect)
	}
}

func TestPostWithTime(t *testing.T) {
	testcases := map[string]Config{
		"with Async": {
			Async:         true,
			MarshalAsJSON: true,
			TagPrefix:     "acme",
		},
		"without Async": {
			Async:         false,
			MarshalAsJSON: true,
			TagPrefix:     "acme",
		},
	}

	for tcname := range testcases {
		t.Run(tcname, func(t *testing.T) {
			tc := testcases[tcname]
			t.Parallel()

			d := newTestDialer()
			var f *Fluent
			defer func() {
				if f != nil {
					f.Close()
				}
			}()

			go func() {
				var err error
				if f, err = newWithDialer(tc, d); err != nil {
					t.Errorf("Unexpected error: %v", err)
				}

				_ = f.PostWithTime("tag_name", time.Unix(1482493046, 0), map[string]string{"foo": "bar"})
				_ = f.PostWithTime("tag_name", time.Unix(1482493050, 0), map[string]string{"fluentd": "is awesome"})
			}()

			conn := d.waitForNewConn(true)
			assertReceived(t,
				conn.waitForNextWrite(true, "aze"),
				"[\"acme.tag_name\",1482493046,{\"foo\":\"bar\"},{}]")

			assertReceived(t,
				conn.waitForNextWrite(true, "zae"),
				"[\"acme.tag_name\",1482493050,{\"fluentd\":\"is awesome\"},{}]")
		})
	}
}

func TestReconnectAndResendAfterTransientFailure(t *testing.T) {
	testcases := map[string]Config{
		"with Async": {
			Async:         true,
			MarshalAsJSON: true,
		},
		"without Async": {
			Async:         false,
			MarshalAsJSON: true,
		},
	}

	for tcname := range testcases {
		t.Run(tcname, func(t *testing.T) {
			tc := testcases[tcname]
			t.Parallel()

			d := newTestDialer()
			var f *Fluent
			defer func() {
				if f != nil {
					f.Close()
				}
			}()

			go func() {
				var err error
				if f, err = newWithDialer(tc, d); err != nil {
					t.Errorf("Unexpected error: %v", err)
				}

				_ = f.EncodeAndPostData("tag_name", time.Unix(1482493046, 0), map[string]string{"foo": "bar"})
				_ = f.EncodeAndPostData("tag_name", time.Unix(1482493050, 0), map[string]string{"fluentd": "is awesome"})
			}()

			// Accept the first connection dialing and write.
			conn := d.waitForNewConn(true)
			assertReceived(t,
				conn.waitForNextWrite(true, ""),
				"[\"tag_name\",1482493046,{\"foo\":\"bar\"},{}]")

			// The next write will fail and the next connection dialing will be dropped
			// to test if the logger is reconnecting as expected.
			conn.waitForNextWrite(false, "")
			d.waitForNewConn(false)

			// Next, we allow a new connection to be established and we allow the last message to be written.
			conn = d.waitForNewConn(true)
			assertReceived(t,
				conn.waitForNextWrite(true, ""),
				"[\"tag_name\",1482493050,{\"fluentd\":\"is awesome\"},{}]")
		})
	}
}

// **Should Close() succeed?**
//
// * CloseOnAsyncConnect: When no logs have been written yet
// * CloseOnAsyncReconnect: When there're pending logs to write
//
// | ForceStopAsyncSend / RequestAck | CloseOnAsyncConnect | CloseOnAsyncReconnect |
// |---------------------------------|---------------------|-----------------------|
// |      `true`        / `true`     | succeeds            | succeeds              |
// |      `true`        / `false`    | succeeds            | succeeds              |
// |      `false`       / `true`     | succeeds            | fails                 |
// |      `false`       / `false`    | succeeds            | fails                 |

func timeout(t *testing.T, duration time.Duration, fn func(), reason string) {
	done := make(chan struct{})
	go func() {
		fn()
		done <- struct{}{}
	}()

	select {
	case <-time.After(duration):
		t.Fatalf("time out after %s: %s", duration.String(), reason)
	case <-done:
		return
	}
}

func TestCloseOnFailingAsyncConnect(t *testing.T) {
	testcases := map[string]Config{
		"with ForceStopAsyncSend and with RequestAck": {
			Async:              true,
			ForceStopAsyncSend: true,
			RequestAck:         true,
		},
		"with ForceStopAsyncSend and without RequestAck": {
			Async:              true,
			ForceStopAsyncSend: true,
			RequestAck:         false,
		},
		"without ForceStopAsyncSend and with RequestAck": {
			Async:              true,
			ForceStopAsyncSend: false,
			RequestAck:         true,
		},
		"without ForceStopAsyncSend and without RequestAck": {
			Async:              true,
			ForceStopAsyncSend: false,
			RequestAck:         false,
		},
	}

	for tcname := range testcases {
		t.Run(tcname, func(t *testing.T) {
			tc := testcases[tcname]
			t.Parallel()

			d := newTestDialer()
			f, err := newWithDialer(tc, d)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			timeout(t, 1*time.Second, func() { f.Close() }, "failed to close the logger")
		})
	}
}

func ackRespMsgp(t *testing.T, ack string) string {
	msg := AckResp{ack}
	buf := &bytes.Buffer{}
	ackW := msgp.NewWriter(buf)
	if err := msg.EncodeMsg(ackW); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	ackW.Flush()
	return buf.String()
}

func TestCloseOnFailingAsyncReconnect(t *testing.T) {
	testcases := map[string]Config{
		"with RequestAck": {
			Async:              true,
			ForceStopAsyncSend: true,
			RequestAck:         true,
		},
		"without RequestAck": {
			Async:              true,
			ForceStopAsyncSend: true,
			RequestAck:         false,
		},
	}

	for tcname := range testcases {
		t.Run(tcname, func(t *testing.T) {
			tc := testcases[tcname]
			t.Parallel()

			d := newTestDialer()
			f, err := newWithDialer(tc, d)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			// Send a first message successfully.
			_ = f.EncodeAndPostData("tag_name", time.Unix(1482493046, 0), map[string]string{"foo": "bar"})
			conn := d.waitForNewConn(true)
			conn.waitForNextWrite(true, ackRespMsgp(t, "dgxdWAAAAABS/fwHIYJlTQ=="))

			// Then try to send one during a transient connection failure.
			_ = f.EncodeAndPostData("tag_name", time.Unix(1482493046, 0), map[string]string{"bar": "baz"})
			conn.waitForNextWrite(false, "")

			// And add some more logs to the log buffer.
			_ = f.EncodeAndPostData("tag_name", time.Unix(1482493046, 0), map[string]string{"acme": "corporation"})

			// But close the logger before it got sent. This is expected to not block.
			timeout(t, 60*time.Second, func() { f.Close() }, "failed to close the logger")
		})
	}
}

func Benchmark_PostWithShortMessage(b *testing.B) {
	b.StopTimer()
	f, err := New(Config{})
	if err != nil {
		panic(err)
	}

	b.StartTimer()
	data := map[string]string{"message": "Hello World"}
	for i := 0; i < b.N; i++ {
		if err := f.Post("tag", data); err != nil {
			panic(err)
		}
	}
}

func Benchmark_PostWithShortMessageMarshalAsJSON(b *testing.B) {
	b.StopTimer()
	f, err := New(Config{MarshalAsJSON: true})
	if err != nil {
		panic(err)
	}

	b.StartTimer()
	data := map[string]string{"message": "Hello World"}
	for i := 0; i < b.N; i++ {
		if err := f.Post("tag", data); err != nil {
			panic(err)
		}
	}
}

func Benchmark_LogWithChunks(b *testing.B) {
	b.StopTimer()
	f, err := New(Config{})
	if err != nil {
		panic(err)
	}

	b.StartTimer()
	data := map[string]string{"msg": "sdfsdsdfdsfdsddddfsdfsdsdfdsfdsddddfsdfsdsdfdsfdsddddfsdfsdsdfdsfdsddddfsdfsdsdfdsfdsddddfsdfsdsdfdsfdsddddfsdfsdsdfdsfdsddddfsdfsdsdfdsfdsddddf"}
	for i := 0; i < b.N; i++ {
		if err := f.Post("tag", data); err != nil {
			panic(err)
		}
	}
}

func Benchmark_PostWithStruct(b *testing.B) {
	b.StopTimer()
	f, err := New(Config{})
	if err != nil {
		panic(err)
	}

	b.StartTimer()
	data := struct {
		Name string `msg:"msgnamename"`
	}{
		"john smith",
	}
	for i := 0; i < b.N; i++ {
		if err := f.Post("tag", data); err != nil {
			panic(err)
		}
	}
}

func Benchmark_PostWithStructTaggedAsCodec(b *testing.B) {
	b.StopTimer()
	f, err := New(Config{})
	if err != nil {
		panic(err)
	}

	b.StartTimer()
	data := struct {
		Name string `codec:"codecname"`
	}{
		"john smith",
	}
	for i := 0; i < b.N; i++ {
		if err := f.Post("tag", data); err != nil {
			panic(err)
		}
	}
}

func Benchmark_PostWithStructWithoutTag(b *testing.B) {
	b.StopTimer()
	f, err := New(Config{})
	if err != nil {
		panic(err)
	}

	b.StartTimer()
	data := struct {
		Name string
	}{
		"john smith",
	}
	for i := 0; i < b.N; i++ {
		if err := f.Post("tag", data); err != nil {
			panic(err)
		}
	}
}

func Benchmark_PostWithMapString(b *testing.B) {
	b.StopTimer()
	f, err := New(Config{})
	if err != nil {
		panic(err)
	}

	b.StartTimer()
	data := map[string]string{
		"foo": "bar",
	}
	for i := 0; i < b.N; i++ {
		if err := f.Post("tag", data); err != nil {
			panic(err)
		}
	}
}

func Benchmark_PostWithMsgpMarshaler(b *testing.B) {
	b.StopTimer()
	f, err := New(Config{})
	if err != nil {
		panic(err)
	}

	b.StartTimer()
	data := &TestMessage{Foo: "bar"}
	for i := 0; i < b.N; i++ {
		if err := f.Post("tag", data); err != nil {
			panic(err)
		}
	}
}

func Benchmark_PostWithMapSlice(b *testing.B) {
	b.StopTimer()
	f, err := New(Config{})
	if err != nil {
		panic(err)
	}

	b.StartTimer()
	data := map[string][]int{
		"foo": {1, 2, 3},
	}
	for i := 0; i < b.N; i++ {
		if err := f.Post("tag", data); err != nil {
			panic(err)
		}
	}
}

func Benchmark_PostWithMapStringAndTime(b *testing.B) {
	b.StopTimer()
	f, err := New(Config{})
	if err != nil {
		panic(err)
	}

	b.StartTimer()
	data := map[string]string{
		"foo": "bar",
	}
	tm := time.Now()
	for i := 0; i < b.N; i++ {
		if err := f.PostWithTime("tag", tm, data); err != nil {
			panic(err)
		}
	}
}
