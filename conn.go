package redis

import (
	"context"
	"net"
)

// Conn represents a connection to a redis server.
type Conn interface {
	Send(ctx context.Context, data DataType) error
	Receive(ctx context.Context) (DataType, error)
	Close(ctx context.Context) error
}

type conn struct {
	conn net.Conn
}

func newConn(nconn net.Conn) Conn {
	return &conn{nconn}
}

func (c *conn) Send(ctx context.Context, data DataType) error {
	panic("implement me")
}

func (c *conn) Receive(ctx context.Context) (DataType, error) {
	panic("implement me")
}

func (c *conn) Close(context.Context) error {
	return c.conn.Close()
}
