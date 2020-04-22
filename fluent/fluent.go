package fluent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	"bytes"
	"encoding/base64"
	"encoding/binary"
	"math/rand"

	"github.com/tinylib/msgp/msgp"
)

const (
	defaultHost                   = "127.0.0.1"
	defaultNetwork                = "tcp"
	defaultSocketPath             = ""
	defaultPort                   = 24224
	defaultTimeout                = 3 * time.Second
	defaultWriteTimeout           = time.Duration(0) // Write() will not time out
	defaultBufferLimit            = 8 * 1024
	defaultRetryWait              = 500
	defaultMaxRetryWait           = 60000
	defaultMaxRetry               = 13
	defaultReconnectWaitIncreRate = 1.5
	// Default sub-second precision value to false since it is only compatible
	// with fluentd versions v0.14 and above.
	defaultSubSecondPrecision = false
)

type Config struct {
	FluentPort         int           `json:"fluent_port"`
	FluentHost         string        `json:"fluent_host"`
	FluentNetwork      string        `json:"fluent_network"`
	FluentSocketPath   string        `json:"fluent_socket_path"`
	Timeout            time.Duration `json:"timeout"`
	WriteTimeout       time.Duration `json:"write_timeout"`
	BufferLimit        int           `json:"buffer_limit"`
	RetryWait          int           `json:"retry_wait"`
	MaxRetry           int           `json:"max_retry"`
	MaxRetryWait       int           `json:"max_retry_wait"`
	TagPrefix          string        `json:"tag_prefix"`
	Async              bool          `json:"async"`
	ForceStopAsyncSend bool          `json:"force_stop_async_send"`
	// Deprecated: Use Async instead
	AsyncConnect  bool `json:"async_connect"`
	MarshalAsJSON bool `json:"marshal_as_json"`

	// Sub-second precision timestamps are only possible for those using fluentd
	// v0.14+ and serializing their messages with msgpack.
	SubSecondPrecision bool `json:"sub_second_precision"`

	// RequestAck sends the chunk option with a unique ID. The server will
	// respond with an acknowledgement. This option improves the reliability
	// of the message transmission.
	RequestAck bool `json:"request_ack"`
}

type ErrUnknownNetwork struct {
	network string
}

func (e *ErrUnknownNetwork) Error() string {
	return "unknown network " + e.network
}

func NewErrUnknownNetwork(network string) error {
	return &ErrUnknownNetwork{network}
}

type msgToSend struct {
	data []byte
	ack  string
}

type Fluent struct {
	Config

	dialer dialer
	// stopRunning is used in async mode to signal to run() and connectOrRetryAsync()
	// they should abort.
	stopRunning chan bool
	// stopAsyncConnect is used by connectOrRetryAsync() to signal to
	// connectOrRetry() it should abort.
	stopAsyncConnect chan bool
	pending          chan *msgToSend
	wg               sync.WaitGroup

	muconn sync.Mutex
	conn   net.Conn
}

type dialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

// New creates a new Logger.
func New(config Config) (*Fluent, error) {
	if config.Timeout == 0 {
		config.Timeout = defaultTimeout
	}
	return newWithDialer(config, &net.Dialer{
		Timeout: config.Timeout,
	})
}

func newWithDialer(config Config, d dialer) (f *Fluent, err error) {
	if config.FluentNetwork == "" {
		config.FluentNetwork = defaultNetwork
	}
	if config.FluentHost == "" {
		config.FluentHost = defaultHost
	}
	if config.FluentPort == 0 {
		config.FluentPort = defaultPort
	}
	if config.FluentSocketPath == "" {
		config.FluentSocketPath = defaultSocketPath
	}
	if config.WriteTimeout == 0 {
		config.WriteTimeout = defaultWriteTimeout
	}
	if config.BufferLimit == 0 {
		config.BufferLimit = defaultBufferLimit
	}
	if config.RetryWait == 0 {
		config.RetryWait = defaultRetryWait
	}
	if config.MaxRetry == 0 {
		config.MaxRetry = defaultMaxRetry
	}
	if config.MaxRetryWait == 0 {
		config.MaxRetryWait = defaultMaxRetryWait
	}
	if config.AsyncConnect {
		fmt.Fprintf(os.Stderr, "fluent#New: AsyncConnect is now deprecated, please use Async instead")
		config.Async = config.Async || config.AsyncConnect
	}

	if config.Async {
		f = &Fluent{
			Config:           config,
			dialer:           d,
			stopRunning:      make(chan bool),
			stopAsyncConnect: make(chan bool),
			pending:          make(chan *msgToSend, config.BufferLimit),
		}
		f.wg.Add(1)
		go f.run()
	} else {
		f = &Fluent{
			Config: config,
			dialer: d,
		}
		err = f.connect(context.Background())
	}
	return
}

// Post writes the output for a logging event.
//
// Examples:
//
//  // send map[string]
//  mapStringData := map[string]string{
//  	"foo":  "bar",
//  }
//  f.Post("tag_name", mapStringData)
//
//  // send message with specified time
//  mapStringData := map[string]string{
//  	"foo":  "bar",
//  }
//  tm := time.Now()
//  f.PostWithTime("tag_name", tm, mapStringData)
//
//  // send struct
//  structData := struct {
//  		Name string `msg:"name"`
//  } {
//  		"john smith",
//  }
//  f.Post("tag_name", structData)
//
func (f *Fluent) Post(tag string, message interface{}) error {
	timeNow := time.Now()
	return f.PostWithTime(tag, timeNow, message)
}

func (f *Fluent) PostWithTime(tag string, tm time.Time, message interface{}) error {
	if len(f.TagPrefix) > 0 {
		tag = f.TagPrefix + "." + tag
	}

	if m, ok := message.(msgp.Marshaler); ok {
		return f.EncodeAndPostData(tag, tm, m)
	}

	msg := reflect.ValueOf(message)
	msgtype := msg.Type()

	if msgtype.Kind() == reflect.Struct {
		// message should be tagged by "codec" or "msg"
		kv := make(map[string]interface{})
		fields := msgtype.NumField()
		for i := 0; i < fields; i++ {
			field := msgtype.Field(i)
			name := field.Name
			if n1 := field.Tag.Get("msg"); n1 != "" {
				name = n1
			} else if n2 := field.Tag.Get("codec"); n2 != "" {
				name = n2
			}
			kv[name] = msg.FieldByIndex(field.Index).Interface()
		}
		return f.EncodeAndPostData(tag, tm, kv)
	}

	if msgtype.Kind() != reflect.Map {
		return errors.New("fluent#PostWithTime: message must be a map")
	} else if msgtype.Key().Kind() != reflect.String {
		return errors.New("fluent#PostWithTime: map keys must be strings")
	}

	kv := make(map[string]interface{})
	for _, k := range msg.MapKeys() {
		kv[k.String()] = msg.MapIndex(k).Interface()
	}

	return f.EncodeAndPostData(tag, tm, kv)
}

func (f *Fluent) EncodeAndPostData(tag string, tm time.Time, message interface{}) error {
	var msg *msgToSend
	var err error
	if msg, err = f.EncodeData(tag, tm, message); err != nil {
		return fmt.Errorf("fluent#EncodeAndPostData: can't convert '%#v' to msgpack:%v", message, err)
	}
	return f.postRawData(msg)
}

// Deprecated: Use EncodeAndPostData instead
func (f *Fluent) PostRawData(msg *msgToSend) {
	f.postRawData(msg)
}

func (f *Fluent) postRawData(msg *msgToSend) error {
	if f.Config.Async {
		return f.appendBuffer(msg)
	}
	// Synchronous write
	return f.write(msg)
}

// For sending forward protocol adopted JSON
type MessageChunk struct {
	message Message
}

// Golang default marshaler does not support
// ["value", "value2", {"key":"value"}] style marshaling.
// So, it should write JSON marshaler by hand.
func (chunk *MessageChunk) MarshalJSON() ([]byte, error) {
	data, err := json.Marshal(chunk.message.Record)
	if err != nil {
		return nil, err
	}
	option, err := json.Marshal(chunk.message.Option)
	if err != nil {
		return nil, err
	}
	return []byte(fmt.Sprintf("[\"%s\",%d,%s,%s]", chunk.message.Tag,
		chunk.message.Time, data, option)), err
}

// getUniqueID returns a base64 encoded unique ID that can be used for chunk/ack
// mechanism, see
// https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#option
func getUniqueID(timeUnix int64) (string, error) {
	buf := bytes.NewBuffer(nil)
	enc := base64.NewEncoder(base64.StdEncoding, buf)
	if err := binary.Write(enc, binary.LittleEndian, timeUnix); err != nil {
		enc.Close()
		return "", err
	}
	if err := binary.Write(enc, binary.LittleEndian, rand.Uint64()); err != nil {
		enc.Close()
		return "", err
	}
	// encoder needs to be closed before buf.String(), defer does not work
	// here
	enc.Close()
	return buf.String(), nil
}

func (f *Fluent) EncodeData(tag string, tm time.Time, message interface{}) (msg *msgToSend, err error) {
	option := make(map[string]string)
	msg = &msgToSend{}
	timeUnix := tm.Unix()
	if f.Config.RequestAck {
		var err error
		msg.ack, err = getUniqueID(timeUnix)
		if err != nil {
			return nil, err
		}
		option["chunk"] = msg.ack
	}
	if f.Config.MarshalAsJSON {
		m := Message{Tag: tag, Time: timeUnix, Record: message, Option: option}
		chunk := &MessageChunk{message: m}
		msg.data, err = json.Marshal(chunk)
	} else if f.Config.SubSecondPrecision {
		m := &MessageExt{Tag: tag, Time: EventTime(tm), Record: message, Option: option}
		msg.data, err = m.MarshalMsg(nil)
	} else {
		m := &Message{Tag: tag, Time: timeUnix, Record: message, Option: option}
		msg.data, err = m.MarshalMsg(nil)
	}
	return
}

// Close closes the connection, waiting for pending logs to be sent
func (f *Fluent) Close() (err error) {
	if f.Config.Async {
		if f.Config.ForceStopAsyncSend {
			f.stopRunning <- true
			close(f.stopRunning)
		}
		close(f.pending)
		f.wg.Wait()
	}

	f.muconn.Lock()
	f.close()
	f.muconn.Unlock()

	return
}

// appendBuffer appends data to buffer with lock.
func (f *Fluent) appendBuffer(msg *msgToSend) error {
	select {
	case f.pending <- msg:
	default:
		return fmt.Errorf("fluent#appendBuffer: Buffer full, limit %v", f.Config.BufferLimit)
	}
	return nil
}

// close closes the connection.
func (f *Fluent) close() {
	if f.conn != nil {
		f.conn.Close()
		f.conn = nil
	}
}

// connect establishes a new connection using the specified transport.
func (f *Fluent) connect(ctx context.Context) (err error) {
	switch f.Config.FluentNetwork {
	case "tcp":
		f.conn, err = f.dialer.DialContext(ctx,
			f.Config.FluentNetwork,
			f.Config.FluentHost+":"+strconv.Itoa(f.Config.FluentPort))
	case "unix":
		f.conn, err = f.dialer.DialContext(ctx,
			f.Config.FluentNetwork,
			f.Config.FluentSocketPath)
	default:
		err = NewErrUnknownNetwork(f.Config.FluentNetwork)
	}

	return err
}

var errIsClosing = errors.New("fluent logger is closing, aborting async connect")

func (f *Fluent) connectOrRetry(ctx context.Context) error {
	// Use a Time channel instead of time.Sleep() to avoid blocking this
	// goroutine during eventually way too much time (because of the exponential
	// back-off retry).
	waiter := time.After(time.Duration(0))
	for i := 0; i < f.Config.MaxRetry; i++ {
		select {
		case <-waiter:
			err := f.connect(ctx)
			if err == nil {
				return nil
			}

			if _, ok := err.(*ErrUnknownNetwork); ok {
				// No need to retry on unknown network error. Thus false is passed
				// to ready channel to let the other end drain the message queue.
				return err
			}

			waitTime := f.Config.RetryWait * e(defaultReconnectWaitIncreRate, float64(i-1))
			if waitTime > f.Config.MaxRetryWait {
				waitTime = f.Config.MaxRetryWait
			}

			waiter = time.After(time.Duration(waitTime) * time.Millisecond)
		case <-f.stopAsyncConnect:
			return errIsClosing
		}
	}

	return fmt.Errorf("could not connect to fluentd after %d retries", f.Config.MaxRetry)
}

// connectOrRetryAsync returns an error when it fails to connect to fluentd or
// when Close() has been called.
func (f *Fluent) connectOrRetryAsync(ctx context.Context) error {
	ctx, cancelDialing := context.WithCancel(ctx)
	errCh := make(chan error)

	f.wg.Add(1)
	go func(ctx context.Context, errCh chan<- error) {
		defer f.wg.Done()
		errCh <- f.connectOrRetry(ctx)
	}(ctx, errCh)

	for {
		select {
		case _, ok := <-f.stopRunning:
			// If f.stopRunning is closed before we got something on errCh,
			// we need to wait a bit more.
			if !ok {
				break
			}

			// Stop any connection dialing and then tell connectOrRetry to stop
			// trying to dial the connection. This has to be done in this
			// specifc order to make sure connectOrRetry() is not blocking on
			// the connection dialing.
			cancelDialing()
			f.stopAsyncConnect <- true
		case err := <-errCh:
			return err
		}
	}
}

// run is the goroutine used to unqueue and write logs in async mode. That
// goroutine is meant to run during the whole life of the Fluent logger.
func (f *Fluent) run() {
	drainEvents := false
	var emitEventDrainMsg sync.Once

	for {
		select {
		case entry, ok := <-f.pending:
			if !ok {
				f.wg.Done()
				return
			}
			if drainEvents {
				emitEventDrainMsg.Do(func() { fmt.Fprintf(os.Stderr, "[%s] Discarding queued events...\n", time.Now().Format(time.RFC3339)) })
				continue
			}
			err := f.write(entry)
			if err == errIsClosing {
				drainEvents = true
			} else if err != nil {
				// TODO: log failing message?
				fmt.Fprintf(os.Stderr, "[%s] Unable to send logs to fluentd, reconnecting...\n", time.Now().Format(time.RFC3339))
			}
		case stopRunning, ok := <-f.stopRunning:
			if stopRunning || !ok {
				drainEvents = true
			}
		}
	}
}

func e(x, y float64) int {
	return int(math.Pow(x, y))
}

func (f *Fluent) write(msg *msgToSend) error {
	// This function is used to ensure muconn is properly locked and unlocked
	// between each retry. This gives the importunity to other goroutines to
	// lock it (e.g. to close the connection).
	writer := func() (bool, error) {
		f.muconn.Lock()
		defer f.muconn.Unlock()

		if f.conn == nil {
			var err error
			if f.Config.Async {
				err = f.connectOrRetryAsync(context.Background())
			} else {
				err = f.connectOrRetry(context.Background())
			}

			if err != nil {
				return false, err
			}
		}

		t := f.Config.WriteTimeout
		if time.Duration(0) < t {
			f.conn.SetWriteDeadline(time.Now().Add(t))
		} else {
			f.conn.SetWriteDeadline(time.Time{})
		}

		_, err := f.conn.Write(msg.data)
		if err != nil {
			f.close()
			return true, err
		}

		// Acknowledgment check
		if msg.ack != "" {
			resp := &AckResp{}
			if f.Config.MarshalAsJSON {
				dec := json.NewDecoder(f.conn)
				err = dec.Decode(resp)
			} else {
				r := msgp.NewReader(f.conn)
				err = resp.DecodeMsg(r)
			}

			if err != nil || resp.Ack != msg.ack {
				f.close()
				return true, err
			}
		}

		return false, nil
	}

	for i := 0; i < f.Config.MaxRetry; i++ {
		if retry, err := writer(); !retry {
			return err
		}
	}

	return fmt.Errorf("fluent#write: failed to write after %d attempts", f.Config.MaxRetry)
}
