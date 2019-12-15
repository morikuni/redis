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

func TestConn_Send(t *testing.T) {
	cases := map[string]struct {
		data Data

		want    string
		wantErr bool
	}{
		"simple string": {
			data: SimpleString("Hello"),

			want:    "+Hello\r\n",
			wantErr: false,
		},
		"error": {
			data: Error("World"),

			want:    "-World\r\n",
			wantErr: false,
		},
		"integer": {
			data: Integer(-123),

			want:    ":-123\r\n",
			wantErr: false,
		},
		"bulk string": {
			data: BulkString(`hello

こんにちは
`),

			want: strings.Join([]string{
				"$23",
				"hello\n\nこんにちは\n",
			}, "\r\n") + "\r\n",
			wantErr: false,
		},
		"bulk string empty": {
			data: BulkString{},

			want:    "$0\r\n",
			wantErr: false,
		},
		"bulk string nil": {
			data: BulkString(nil),

			want:    "$-1\r\n",
			wantErr: false,
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

			want: strings.Join([]string{
				"*5",
				"+Hello",
				"-World",
				":-123",
				"$23",
				"hello\n\nこんにちは\n",
				"*1",
				"+Nested",
			}, "\r\n") + "\r\n",
			wantErr: false,
		},
		"array nil": {
			data: Array(nil),

			want:    "*-1\r\n",
			wantErr: false,
		},
		"array empty": {
			data: Array{},

			want:    "*0\r\n",
			wantErr: false,
		},
		"nil": {
			data: nil,

			want:    "",
			wantErr: true,
		},
	}

	for name, tc := range cases {
		tc := tc

		t.Run(name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			conn := newConn(netConn{buf})

			err := conn.Send(context.Background(), tc.data)
			assert.WantError(t, tc.wantErr, err)
			assert.Equal(t, tc.want, buf.String())
		})
	}

}
