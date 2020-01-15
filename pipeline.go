package redis

import (
	"context"
)

type Pipeline struct {
	conn     Conn
	pool     *Pool
	canReuse bool

	pendingResponses []Response
}

func newPipeline(conn Conn, pool *Pool) *Pipeline {
	return &Pipeline{
		conn,
		pool,
		true,
		make([]Response, 0, 5),
	}
}

func (p *Pipeline) updateReuse(err error) {
	if p.canReuse {
		p.canReuse = canReuse(err)
	}
}

func (p *Pipeline) send(ctx context.Context, req Request) error {
	d, err := req.ToData()
	if err != nil {
		return err
	}

	err = p.conn.Send(ctx, d)
	if err != nil {
		p.updateReuse(err)
		return err
	}

	return nil
}

func (p *Pipeline) receive(ctx context.Context, res Response) error {
	d, err := p.conn.Receive(ctx)
	if err != nil {
		p.updateReuse(err)
		return err
	}

	return res.FromData(d)
}

func (p *Pipeline) flush(ctx context.Context) error {
	err := p.conn.Flush(ctx)
	if err != nil {
		p.updateReuse(err)
		return err
	}

	return nil
}

func (p *Pipeline) Do(ctx context.Context, req Request, res Response) error {
	err := p.async(ctx, req, res)
	if err != nil {
		return err
	}

	return p.Await(ctx)
}

func (p *Pipeline) async(ctx context.Context, req Request, res Response) error {
	err := p.send(ctx, req)
	if err != nil {
		return err
	}

	p.pendingResponses = append(p.pendingResponses, res)
	return nil
}

func (p *Pipeline) Async() Doer {
	return doFunc(p.async)
}

func (p *Pipeline) Await(ctx context.Context) error {
	err := p.flush(ctx)
	if err != nil {
		return err
	}

	var resErr error
	for _, res := range p.pendingResponses {
		err := p.receive(ctx, res)
		if err != nil {
			if !p.canReuse {
				return err
			}
			// Continue if canReuse because it maybe a problem of res.FromData.
			// Unless conn error, the connection can be reused.
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

func (p *Pipeline) Multi(ctx context.Context) (*Transaction, error) {
	err := p.Do(ctx, multi, Discard)
	if err != nil {
		return nil, err
	}

	return &Transaction{
		p,
		nil,
	}, nil
}
