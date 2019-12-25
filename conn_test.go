package redis

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/morikuni/redis/internal/assert"
)

type netConn struct {
	*bytes.Buffer
}

func (c netConn) LocalAddr() net.Addr {
	panic("implement me")
}

func (c netConn) RemoteAddr() net.Addr {
	panic("implement me")
}

func (c netConn) SetDeadline(t time.Time) error {
	panic("implement me")
}

func (c netConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (c netConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func (c netConn) Close() error {
	return nil
}

func TestConn(t *testing.T) {
	cases := map[string]struct {
		data Data
		text string

		wantErrSend    bool
		wantErrReceive bool
	}{
		"simple string": {
			data: SimpleString("Hello"),
			text: "+Hello\r\n",

			wantErrSend:    false,
			wantErrReceive: false,
		},
		"error": {
			data: Error("World"),
			text: "-World\r\n",

			wantErrSend:    false,
			wantErrReceive: false,
		},
		"integer": {
			data: Integer(-123),
			text: ":-123\r\n",

			wantErrSend:    false,
			wantErrReceive: false,
		},
		"bulk string": {
			data: BulkString(`hello

こんにちは
`),

			text: strings.Join([]string{
				"$23",
				"hello\n\nこんにちは\n",
			}, "\r\n") + "\r\n",

			wantErrSend:    false,
			wantErrReceive: false,
		},
		"bulk string empty": {
			data: BulkString{},
			text: "$0\r\n",

			wantErrSend:    false,
			wantErrReceive: false,
		},
		"bulk string nil": {
			data: BulkString(nil),
			text: "$-1\r\n",

			wantErrSend:    false,
			wantErrReceive: false,
		},
		"array": {
			data: Array{
				SimpleString("Hello"),
				Error("World"),
				Integer(-123),
				BulkString(`hello

こんにちは
`),
				Array{
					SimpleString("Nested"),
				},
			},
			text: strings.Join([]string{
				"*5",
				"+Hello",
				"-World",
				":-123",
				"$23",
				"hello\n\nこんにちは\n",
				"*1",
				"+Nested",
			}, "\r\n") + "\r\n",

			wantErrSend:    false,
			wantErrReceive: false,
		},
		"array nil": {
			data: Array(nil),
			text: "*-1\r\n",

			wantErrSend:    false,
			wantErrReceive: false,
		},
		"array empty": {
			data: Array{},
			text: "*0\r\n",

			wantErrSend:    false,
			wantErrReceive: false,
		},
		"nil": {
			data: nil,
			text: "",

			wantErrSend:    true,
			wantErrReceive: true,
		},
	}

	for name, tc := range cases {
		tc := tc

		t.Run(name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			conn := newConn(netConn{buf})

			err := conn.Send(context.Background(), tc.data)
			assert.WantError(t, tc.wantErrSend, err)
			if !tc.wantErrSend {
				assert.Equal(t, tc.text, buf.String())
			}

			buf.Reset()
			buf.WriteString(tc.text)

			res, err := conn.Receive(context.Background())
			assert.WantError(t, tc.wantErrReceive, err)
			if !tc.wantErrReceive {
				assert.Equal(t, tc.data, res)
			}
		})
	}

}
