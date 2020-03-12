// Package dbreaker provides a simple "circuit breaker" wrapper for sql.DB connections
//
// It wraps the original driver with a new driver that exposes a Disable function that
// can toggle access to the database. This allows for stopping db operations for
// maintence purposes without modifying application code
package dbreaker

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
)

// ErrDown is returned when circuit breaker is enabled
var ErrDown = fmt.Errorf("database is down")

// ErrContext is returned when context operations are not supported
var ErrContext = fmt.Errorf("context operations are not supported")

// Downer is an sql driver that can be disabled
type Downer interface {
	driver.Driver
	Disable(bool)
}

// NewDriver registers and returns a driver wrapper that can control access to the inner driver
func NewDriver(name, native string) (Downer, error) {
	for _, d := range sql.Drivers() {
		if d == name {
			return nil, fmt.Errorf("driver %q is already registered", name)
		}
	}
	drv := &Breaker{
		native: native,
		dbs:    make(map[string]*sql.DB),
	}
	sql.Register(name, drv)
	return drv, nil
}

// Breaker is an sql.Driver that can block access to the database
type Breaker struct {
	down   bool   // set true to disable access via this driver
	native string // native sql driver
	dbs    map[string]*sql.DB
}

// Conn implements the sql.Driver.Conn interface
type Conn struct {
	c    driver.Conn
	b    driver.ConnBeginTx
	db   *sql.DB
	down func() bool
}

// Disable allows changing if dribver is enabled
func (w *Breaker) Disable(off bool) {
	w.down = off
}

// Open satisfies the sql.Driver interface
func (w *Breaker) Open(name string) (driver.Conn, error) {
	if w.down {
		return nil, ErrDown
	}
	db, ok := w.dbs[name]
	if !ok {
		var err error
		db, err = sql.Open(w.native, name)
		if err != nil {
			return nil, err
		}
		w.dbs[name] = db
	}

	c, err := db.Driver().Open(name)
	if err != nil {
		return nil, err
	}
	down := func() bool {
		return w.down
	}
	b, _ := c.(driver.ConnBeginTx)
	return &Conn{b: b, c: c, down: down}, nil
}

// Prepare satisfies the sql.driver.Conn interface
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	if c.down() {
		return nil, ErrDown
	}
	return c.c.Prepare(query)

}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
func (c *Conn) Close() error {
	return c.c.Close()
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
func (c *Conn) Begin() (driver.Tx, error) {
	if c.down() {
		return nil, ErrDown
	}
	return c.c.Begin()
}

// BeginTx starts and returns a new transaction using a context.
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.down() {
		return nil, ErrDown
	}
	if c.b == nil {
		return nil, ErrContext
	}
	return c.b.BeginTx(ctx, opts)
}
