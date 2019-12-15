package redis

import (
	"bufio"
	"context"
	"fmt"
	"net"
)

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

func (c *conn) Send(ctx context.Context, data Data) error {
	c.w.Reset(c.conn)

	if dl, ok := ctx.Deadline(); ok {
		err := c.conn.SetWriteDeadline(dl)
		if err != nil {
			return err
		}
	}

	err := c.send(ctx, data)
	if err != nil {
		return err
	}

	return c.w.Flush()
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
	panic("implement me")
}

func (c *conn) Close(context.Context) error {
	return c.conn.Close()
}
