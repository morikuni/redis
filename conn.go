package redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
)

func newConnError(err error, reuse bool) error {
	return &ConnError{err, reuse}
}

type ConnError struct {
	err   error
	reuse bool
}

func (e *ConnError) Error() string {
	return fmt.Sprintf("conn error: %s", e.err)
}

func (e *ConnError) Unwrap() error {
	return e.err
}

// CanReuse returns whether the connection is available for
// the next operation.
func (e *ConnError) CanReuse() bool {
	return e.reuse
}

// Conn represents a connection to a redis server.
type Conn interface {
	Send(ctx context.Context, data Data) error
	Receive(ctx context.Context) (Data, error)
	Close(ctx context.Context) error
}

type conn struct {
	conn net.Conn
	w    *bufio.Writer
	r    *bufio.Reader
}

func newConn(nconn net.Conn) Conn {
	return &conn{
		nconn,
		bufio.NewWriterSize(nconn, 1024),
		bufio.NewReaderSize(nconn, 1024),
	}
}

// Send writes data to connection.
// It is not supporting canceling by context because canceling connection
// requires closing connection. It makes conn to create new connection and
// it will cause TCP hand shake. Since Redis should be fast enough
func (c *conn) Send(ctx context.Context, data Data) error {
	if err := ctx.Err(); err != nil {
		return newConnError(err, true)
	}

	if dl, ok := ctx.Deadline(); ok {
		err := c.conn.SetWriteDeadline(dl)
		if err != nil {
			return newConnError(err, isTemporary(err))
		}
	}

	err := c.send(ctx, data)
	if err != nil {
		if isTimeout(err) && ctx.Err() == context.DeadlineExceeded {
			return newConnError(context.DeadlineExceeded, false)
		}

		return newConnError(err, false)
	}

	err = c.w.Flush()
	if err != nil {
		if isTimeout(err) && ctx.Err() == context.DeadlineExceeded {
			return newConnError(context.DeadlineExceeded, false)
		}

		return newConnError(err, false)
	}

	return nil
}

func (c *conn) send(ctx context.Context, data Data) error {
	switch t := data.(type) {
	case SimpleString:
		_, err := fmt.Fprintf(c.w, "+%s\r\n", t)
		return err
	case Error:
		_, err := fmt.Fprintf(c.w, "-%s\r\n", t)
		return err
	case Integer:
		_, err := fmt.Fprintf(c.w, ":%d\r\n", t)
		return err
	case BulkString:
		if t == nil {
			_, err := c.w.WriteString("$-1\r\n")
			return err
		}

		l := len(t)
		_, err := fmt.Fprintf(c.w, "$%d\r\n", l)
		if err != nil {
			return err
		}

		if l == 0 {
			return nil
		}

		_, err = c.w.Write(t)
		if err != nil {
			return err
		}

		_, err = c.w.WriteString("\r\n")
		return err
	case Array:
		if t == nil {
			_, err := c.w.WriteString("*-1\r\n")
			return err
		}

		l := len(t)
		_, err := fmt.Fprintf(c.w, "*%d\r\n", l)
		if err != nil {
			return err
		}

		for _, elem := range t {
			err := c.send(ctx, elem)
			if err != nil {
				return err
			}
		}

		return nil
	default:
		return fmt.Errorf("unknown type: %T(%v)", data, data)
	}
}

func (c *conn) Receive(ctx context.Context) (Data, error) {
	if dl, ok := ctx.Deadline(); ok {
		err := c.conn.SetReadDeadline(dl)
		if err != nil {
			return nil, newConnError(err, false)
		}
	}

	data, err := c.receive(ctx)
	if err != nil {
		if isTimeout(err) && ctx.Err() == context.DeadlineExceeded {
			return nil, newConnError(context.DeadlineExceeded, false)
		}

		return nil, newConnError(err, false)
	}

	return data, nil
}

func (c *conn) receive(ctx context.Context) (Data, error) {
	line, _, err := c.r.ReadLine()
	if err != nil {
		return nil, err
	}

	if len(line) == 0 {
		return nil, errors.New("empty line response")
	}

	dataType, line := line[0], line[1:]

	switch dataType {
	case '+':
		return SimpleString(line), nil
	case '-':
		return Error(line), nil
	case ':':
		i, err := strconv.ParseInt(string(line), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse integer: %s: %s", err, string(line))
		}

		return Integer(i), nil
	case '$':
		l, err := strconv.ParseInt(string(line), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse length of bulk string: %s: %s", err, string(line))
		}

		if l == 0 {
			return BulkString{}, nil
		}

		if l == -1 {
			return BulkString(nil), nil
		}

		str := make([]byte, l+2) // +2 for \r\n
		_, err = io.ReadFull(c.r, str)
		if err != nil {
			return nil, fmt.Errorf("read bulk string: %s", err)
		}

		return BulkString(str[:l]), nil
	case '*':
		l, err := strconv.ParseInt(string(line), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse length of array: %s: %s", err, string(line))
		}

		if l == 0 {
			return Array{}, nil
		}

		if l == -1 {
			return Array(nil), nil
		}

		res := make(Array, l)
		for i := int64(0); i < l; i++ {
			elem, err := c.receive(ctx)
			if err != nil {
				return nil, err
			}

			res[i] = elem
		}

		return res, nil
	default:
		return nil, fmt.Errorf("unknown data type %q: %s%s", dataType, string(dataType), line)
	}
}

func (c *conn) Close(context.Context) error {
	return c.conn.Close()
}

func canReuse(err error) bool {
	if err == nil {
		return true
	}

	ce, ok := err.(*ConnError)
	if ok {
		return ce.CanReuse()
	}

	// Basically, errors are application error unless ConnError.
	// Therefore connection can be reused.
	return true
}

func isTemporary(err error) bool {
	te, ok := err.(interface{ Temporary() bool })
	if ok {
		return te.Temporary()
	}

	return false
}

func isTimeout(err error) bool {
	to, ok := err.(interface{ Timeout() bool })
	if ok {
		return to.Timeout()
	}

	return false
}
