package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	spool "github.com/morikuni/slice/pool"
)

type Pool struct {
	idles   []Conn
	pool    *spool.Pool
	conf    *poolConfig
	mu      sync.Mutex
	numOpen int64
	started bool
}

type poolConfig struct {
	addr        string
	dialFunc    func(ctx context.Context, network, addr string) (net.Conn, error)
	onError     func(context.Context, error)
	maxOpen     int64
	maxIdle     int64
	minIdle     int64
	idleTimeout time.Duration
}

type PoolOption func(*poolConfig)

func MaxIdle(n int) PoolOption {
	return func(p *poolConfig) {
		p.maxIdle = int64(n)
	}
}

func evaluatePoolOption(addr string, opts []PoolOption) (*poolConfig, error) {
	conf := &poolConfig{
		addr:        addr,
		dialFunc:    (&net.Dialer{}).DialContext,
		maxOpen:     0, // no limit
		maxIdle:     int64(10 * runtime.NumCPU()),
		minIdle:     int64(runtime.NumCPU()),
		idleTimeout: time.Minute,
		onError: func(ctx context.Context, err error) {
			fmt.Printf("RedisPoolError: %v\n", err)
		},
	}

	for _, o := range opts {
		o(conf)
	}

	if conf.addr == "" {
		return nil, errors.New("addr must not be empty")
	}

	if conf.maxOpen < 0 {
		return nil, fmt.Errorf("max open must not be less than 0 but got %d", conf.maxOpen)
	}

	if conf.dialFunc == nil {
		return nil, errors.New("dial func must not be nil")
	}

	if conf.onError == nil {
		return nil, errors.New("on error must not be nil")
	}

	return conf, nil
}

func NewPool(addr string, opts ...PoolOption) (*Pool, error) {
	conf, err := evaluatePoolOption(addr, opts)
	if err != nil {
		return nil, err
	}

	idles := make([]Conn, conf.maxIdle)
	pl, err := spool.New(len(idles),
		spool.MinIdle(int(conf.minIdle)),
		spool.IdleTimeout(conf.idleTimeout),
	)
	if err != nil {
		return nil, err
	}

	return &Pool{
		idles: idles,
		pool:  pl,
		conf:  conf,
	}, nil
}

func (p *Pool) getIdle(ctx context.Context) (Conn, bool) {
	idx, ok := p.pool.Get()
	if ok {
		return p.idles[idx], true
	}

	return nil, false
}

func (p *Pool) Get(ctx context.Context) (Conn, error) {
	p.mu.Lock()

	conn, ok := p.getIdle(ctx)
	if ok {
		p.mu.Unlock()
		return conn, nil
	}

	if !p.canOpenNewConn() {
		p.mu.Unlock()
		return nil, errors.New("cannot open new conn due to max open limit")
	}

	p.addNumOpen(1)
	p.mu.Unlock()

	// unlock mutex because dial take time.

	conn, err := p.dial(ctx)
	if err != nil {
		p.addNumOpen(-1)
		return nil, err
	}

	return conn, nil
}

func (p *Pool) canOpenNewConn() bool {
	if p.conf.maxOpen == 0 {
		return true
	}

	return p.getNumOpen() < p.conf.maxOpen
}

func (p *Pool) getNumOpen() int64 {
	return atomic.LoadInt64(&p.numOpen)
}

func (p *Pool) addNumOpen(d int64) {
	atomic.AddInt64(&p.numOpen, d)
}

func (p *Pool) Put(ctx context.Context, conn Conn) error {
	p.mu.Lock()
	idx, ok := p.pool.Put()
	if ok {
		p.idles[idx] = conn
		p.mu.Unlock()
		return nil
	}
	p.mu.Unlock()

	return conn.Close(ctx)
}

func (p *Pool) Close(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var err error
	for {
		conn, ok := p.getIdle(ctx)
		if !ok {
			break
		}
		if e := conn.Close(ctx); e != nil {
			err = e
		}
	}

	return err
}

func (p *Pool) init(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started {
		return errors.New("pool has already started")
	}

	for i := p.getNumOpen(); i < p.conf.minIdle; i++ {
		conn, err := p.dial(ctx)
		if err != nil {
			return err
		}

		p.addNumOpen(1)
		idx, ok := p.pool.Put()
		if ok {
			p.idles[idx] = conn
		}
	}

	return nil
}

func (p *Pool) Start(ctx context.Context) error {
	if err := p.init(ctx); err != nil {
		return err
	}

	for {
		p.mu.Lock()
		idx, ok, next := p.pool.CloseIdle()
		if ok {
			err := p.idles[idx].Close(ctx)
			p.mu.Unlock()
			if err != nil {
				p.onError(ctx, err)
			}
		} else {
			p.mu.Unlock()
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Until(next)):
		}
	}
}

func (p *Pool) onError(ctx context.Context, err error) {
	if p.conf.onError != nil {
		p.conf.onError(ctx, err)
	}
}

func (p *Pool) dial(ctx context.Context) (Conn, error) {
	conn, err := p.conf.dialFunc(ctx, "tcp", p.conf.addr)
	if err != nil {
		return nil, err
	}

	return newConn(conn), nil
}

func newPoolConn(conn Conn, p *Pool) Conn {
	return &poolConn{conn, p, false}
}

type poolConn struct {
	Conn
	pool   *Pool
	closed bool
}

func (c *poolConn) Close(ctx context.Context) error {
	if !c.closed {
		c.pool.addNumOpen(-1)
	}

	return nil
}
