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
