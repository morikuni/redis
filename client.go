package redis

import (
	"context"
	"net"
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

func (c *Client) Do(ctx context.Context, req Request, res Response) (retError error) {
	pipe, err := c.Pipeline(ctx)
	if err != nil {
		return err
	}
	defer func() {
		err := pipe.Close(ctx)
		if retError != nil {
			retError = err
		}
	}()

	return pipe.Do(ctx, req, res)
}

func (c *Client) Pipeline(ctx context.Context) (*Pipeline, error) {
	conn, err := c.pool.Get(ctx)
	if err != nil {
		return nil, err
	}

	return &Pipeline{
		conn:     conn,
		pool:     c.pool,
		canReuse: true,
	}, nil
}

type Pipeline struct {
	conn     Conn
	pool     *Pool
	canReuse bool
}

func (p *Pipeline) Send(ctx context.Context, req Request) error {
	d, err := req.ToData()
	if err != nil {
		return err
	}

	err = p.conn.Send(ctx, d)
	if err != nil {
		p.canReuse = canReuse(err)
		return err
	}

	return nil
}

func (p *Pipeline) Receive(ctx context.Context, res Response) error {
	d, err := p.conn.Receive(ctx)
	if err != nil {
		p.canReuse = canReuse(err)
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

func (p *Pipeline) Close(ctx context.Context) error {
	if p.canReuse {
		return p.pool.Put(ctx, p.conn)
	}

	return p.conn.Close(ctx)
}
