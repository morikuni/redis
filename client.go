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

	conn, err := c.pool.Get(ctx)
	if err != nil {
		return err
	}

	err = conn.Send(ctx, sd)
	if err != nil {
		return putConn(ctx, err, conn, c.pool)
	}

	rd, err := conn.Receive(ctx)
	if err != nil {
		return putConn(ctx, err, conn, c.pool)
	}

	err = c.pool.Put(ctx, conn)
	if err != nil {
		return err
	}

	err = res.FromData(rd)
	if err != nil {
		return err
	}

	return nil
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

func (c *Client) Get(ctx context.Context, req *GetRequest) (*StringResponse, error) {
	var res StringResponse
	return &res, c.Do(ctx, req, &res)
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
		d = append(d, BulkString("PX"), Integer(req.Expire.Milliseconds()))
	}

	switch {
	case req.NotExist:
		d = append(d, BulkString("NX"))
	case req.AlreadyExist:
		d = append(d, BulkString("XX"))
	}

	return d, nil
}

func (c *Client) Set(ctx context.Context, req *SetRequest) (*StringResponse, error) {
	var res StringResponse
	return &res, c.Do(ctx, req, &res)
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

func (c *Client) Incr(ctx context.Context, req *IncrRequest) (*IntegerResponse, error) {
	var res IntegerResponse
	return &res, c.Do(ctx, req, &res)
}
