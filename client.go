package redis

import (
	"context"
	"net"
	"time"
)

type config struct {
	dialFunc func(ctx context.Context, network, address string) (net.Conn, error)
}

type Client struct {
	pool *Pool
}

func NewClient(pool *Pool) *Client {
	return &Client{pool}
}

type Request interface {
	ToData() (Data, error)
}

type Response interface {
	FromData(data Data) error
}

func putConn(ctx context.Context, err error, conn Conn, pool *Pool) error {
	if err == nil {
		return pool.Put(ctx, conn)
	}

	if canReuse(err) {
		_ = pool.Put(ctx, conn)
		return err
	}

	_ = conn.Close(ctx)
	return err
}

func (c *Client) Do(ctx context.Context, req Request, res Response) error {
	sd, err := req.ToData()
	if err != nil {
		return err
	}

	var rd Data
	err = c.withConn(ctx, func(ctx context.Context, conn Conn) error {
		err = conn.Send(ctx, sd)
		if err != nil {
			return err
		}

		rd, err = conn.Receive(ctx)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	err = res.FromData(rd)
	if err != nil {
		return err
	}

	return nil
}

type Pipeline struct {
	conn Conn
}

func (p *Pipeline) Send(ctx context.Context, req Request) error {
	d, err := req.ToData()
	if err != nil {
		return err
	}

	return p.conn.Send(ctx, d)
}

func (p *Pipeline) Receive(ctx context.Context, res Response) error {
	d, err := p.conn.Receive(ctx)
	if err != nil {
		return err
	}

	return res.FromData(d)
}

func (p *Pipeline) Do(ctx context.Context, req Request, res Response) error {
	err := p.Send(ctx, req)
	if err != nil {
		return err
	}

	return p.Receive(ctx, res)
}

func (c *Client) withConn(ctx context.Context, f func(ctx context.Context, conn Conn) error) (retError error) {
	conn, err := c.pool.Get(ctx)
	if err != nil {
		return err
	}

	defer func() {
		err := putConn(ctx, retError, conn, c.pool)
		if retError == nil {
			retError = err
		}
	}()

	return f(ctx, conn)
}

func (c *Client) withPipeline(ctx context.Context, f func(ctx context.Context, p *Pipeline) error) error {
	return c.withConn(ctx, func(ctx context.Context, conn Conn) error {
		return f(ctx, &Pipeline{conn})
	})
}

type GetRequest struct {
	Key string
}

func (req *GetRequest) ToData() (Data, error) {
	return Array{
		BulkString("GET"),
		BulkString(req.Key),
	}, nil
}

type Doer interface {
	Do(ctx context.Context, req Request, res Response) error
}

func Get(ctx context.Context, do Doer, req *GetRequest) (*StringResponse, error) {
	var res StringResponse
	return &res, do.Do(ctx, req, &res)
}

type SetRequest struct {
	Key          string
	Value        string
	Expire       time.Duration
	NotExist     bool
	AlreadyExist bool
}

func (req *SetRequest) ToData() (Data, error) {
	d := Array{
		BulkString("SET"),
		BulkString(req.Key),
		BulkString(req.Value),
	}

	if req.Expire > 0 {
		d = append(d, BulkString("PX"), Integer(req.Expire)/Integer(time.Millisecond))
	}

	switch {
	case req.NotExist:
		d = append(d, BulkString("NX"))
	case req.AlreadyExist:
		d = append(d, BulkString("XX"))
	}

	return d, nil
}

func Set(ctx context.Context, do Doer, req *SetRequest) (*StringResponse, error) {
	var res StringResponse
	return &res, do.Do(ctx, req, &res)
}

type IncrRequest struct {
	Key string
}

func (req *IncrRequest) ToData() (Data, error) {
	return Array{
		BulkString("INCR"),
		BulkString(req.Key),
	}, nil
}

func Incr(ctx context.Context, do Doer, req *IncrRequest) (*IntegerResponse, error) {
	var res IntegerResponse
	return &res, do.Do(ctx, req, &res)
}
