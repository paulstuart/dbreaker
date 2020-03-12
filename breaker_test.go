package dbreaker

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func read(db *sql.DB) error {
	const q = "select first_name, last_name from users where last_name = :last_name"
	rows, err := db.QueryContext(context.Background(), q, sql.Named("last_name", "ramone"))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var first, last string
		if err := rows.Scan(&first, &last); err != nil {
			return err
		}
	}
	return nil
}

func write(ctx context.Context, db *sql.DB, query string, args ...interface{}) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.ExecContext(ctx, args...)
	if err != nil {
		tx.Rollback()
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func TestBreaker(t *testing.T) {
	const (
		driver  = "wrapper"
		create  = "create table if not exists users (id integer primary key, first_name text, last_name text)"
		insert  = "insert into users (first_name, last_name) values('joey','ramone')"
		prepare = "insert into users (first_name, last_name) values(:first,:last)"
		moar    = "insert into users (first_name, last_name) values('johnny','thunders')"
	)
	ctx := context.Background()
	breaker, err := NewDriver(driver, "sqlite3")
	if err != nil {
		t.Fatal(err)
	}

	// set it up

	t.Log("open db")
	db, err := sql.Open(driver, "test.db")
	if err != nil {
		t.Fatal("oops:", err)
	}
	defer db.Close()

	t.Log("create table")
	if _, err := db.ExecContext(ctx, create); err != nil {
		t.Fatal("exec fail:", err)
	}

	// populate

	t.Log("insert a record")
	if r, err := db.ExecContext(ctx, insert); err != nil {
		t.Fatal("exec fail:", err)
	} else {
		cnt, _ := r.RowsAffected()
		if cnt != 1 {
			t.Fatalf("expected row count of 1 but got: %d", cnt)
		}
	}
	if err := write(ctx, db, prepare, "johnny", "rotten"); err != nil {
		t.Fatal("write failed:", err)
	}

	// disable database access and ensure writes/reads are blocked

	breaker.Disable(true)
	t.Log("try to insert another record with db disabled")
	if _, err := db.ExecContext(ctx, moar); err == nil {
		t.Fatal("expected error but got nil")
	} else {
		t.Log("got expected error:", err)
	}
	t.Log("try to read with db disabled")
	if err = read(db); err == nil {
		t.Fatal("expected error but got nil")
	} else {
		t.Log("got expected error:", err)
	}

	// re-enable db and ensure writes/reads work again

	breaker.Disable(false)
	t.Log("try to write with db re-enabled")
	if _, err := db.Exec(moar); err != nil {
		t.Fatal(err)
	}
	if err := write(ctx, db, prepare, "lou", "reed"); err != nil {
		t.Fatal("write failed:", err)
	}
	t.Log("try to read with db re-enabled")
	if err = read(db); err != nil {
		t.Fatal("read error:", err)
	}
}
