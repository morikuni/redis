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

	pendingResponses []Response
}

func (p *Pipeline) send(ctx context.Context, req Request) error {
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

func (p *Pipeline) receive(ctx context.Context, res Response) error {
	d, err := p.conn.Receive(ctx)
	if err != nil {
		p.canReuse = canReuse(err)
		return err
	}

	return res.FromData(d)
}

func (p *Pipeline) Do(ctx context.Context, req Request, res Response) error {
	err := p.send(ctx, req)
	if err != nil {
		return err
	}

	err = p.conn.Flush(ctx)
	if err != nil {
		return err
	}

	return p.receive(ctx, res)
}

func (p *Pipeline) Async() Doer {
	return doFunc(func(ctx context.Context, req Request, res Response) error {
		err := p.send(ctx, req)
		if err != nil {
			return err
		}

		p.pendingResponses = append(p.pendingResponses, res)
		return nil
	})
}

func (p *Pipeline) Await(ctx context.Context) error {
	err := p.conn.Flush(ctx)
	if err != nil {
		return err
	}

	var resErr error
	for _, res := range p.pendingResponses {
		err := p.receive(ctx, res)
		if err != nil {
			if _, ok := err.(*ConnError); !ok {
				return err
			}
			// continue if ConnError because it maybe a problem of res.FromData.
			// unless conn error, the connection can be reused.
			resErr = err
		}
	}

	p.pendingResponses = p.pendingResponses[:0]

	return resErr
}

func (p *Pipeline) Close(ctx context.Context) error {
	if p.canReuse {
		return p.pool.Put(ctx, p.conn)
	}

	return p.conn.Close(ctx)
}
