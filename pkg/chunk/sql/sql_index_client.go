package sql

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/util"
)

type Config struct {
	DSN    string
	Driver string
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.DSN, "sql.dsn", "", "DSN for connecting to the database")
	f.StringVar(&cfg.Driver, "sql.driver", "postgresql", "The database driver to use")
}

// StorageClient implements chunk.IndexClient for SQL databases.
type StorageClient struct {
	db  *sql.DB
	cfg Config
}

var (
	// assert the StorageClient implements chunk.IndexClient
	_ chunk.IndexClient = &StorageClient{}
)

// NewStorageClient returns a new StorageClient.
func NewStorageClient(cfg Config) (*StorageClient, error) {
	db, err := sql.Open(cfg.Driver, cfg.DSN)
	if err != nil {
		return nil, err
	}

	return &StorageClient{
		db:  db,
		cfg: cfg,
	}, nil
}

func (s *StorageClient) createTable(tableName string) error {
	switch s.cfg.Driver {
	case "mysql":
		_, err := s.db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		hash VARCHAR,
		range BLOB,
		value BLOB,
		PRIMARY KEY(hash)
		)`, tableName))
		if err != nil {
			return errors.Wrap(err, "create table failed")
		}
		_, err = s.db.Exec(fmt.Sprintf(`
		CREATE INDEX range_index ON %s (range);
		CREATE INDEX value_index ON %s (value);
		`, tableName, tableName))
		// TODO handle index exists
		if err != nil {
			return errors.Wrap(err, "create index failed")
		}
	case "postgresl":
		_, err := s.db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		hash VARCHAR,
		range BYTEA,
		value BYTEA,
		PRIMARY KEY(hash)
		)`, tableName))
		if err != nil {
			return errors.Wrap(err, "create table failed")
		}
		_, err = s.db.Exec(fmt.Sprintf(`
		CREATE INDEX range_index ON %s (range);
		CREATE INDEX value_index ON %s (value);
		`, tableName, tableName))
		// TODO handle index exists
		if err != nil {
			return errors.Wrap(err, "create index failed")
		}
	}
	return nil
}

// Stop implements chunk.IndexClient.
func (s *StorageClient) Stop() {
	s.db.Close()
}

type writeBatch struct {
	entries []chunk.IndexEntry
}

// NewWriteBatch implements chunk.IndexClient.
func (s *StorageClient) NewWriteBatch() chunk.WriteBatch {
	return &writeBatch{}
}

func (b *writeBatch) Add(tableName, hashValue string, rangeValue []byte, value []byte) {
	b.entries = append(b.entries, chunk.IndexEntry{
		TableName:  tableName,
		HashValue:  hashValue,
		RangeValue: rangeValue,
		Value:      value,
	})
}

// BatchWrite implements chunk.IndexClient.
func (s *StorageClient) BatchWrite(ctx context.Context, batch chunk.WriteBatch) error {
	b := batch.(*writeBatch)

	for _, entry := range b.entries {
		err := s.createTable(entry.TableName)
		if err != nil {
			return errors.WithStack(err)
		}

		var query string
		switch s.cfg.Driver {
		case "mysql":
			query = fmt.Sprintf("INSERT INTO %s (hash, range, value) VALUES ('%s', x'%x', x'%x')", entry.TableName, entry.HashValue, entry.RangeValue, entry.Value)
		case "postgresql":
			query = fmt.Sprintf("INSERT INTO %s (hash, range, value) VALUES ('%s', decode('%s', 'hex'), decode('%s', 'hex'))", entry.TableName, entry.HashValue, entry.RangeValue, entry.Value)
		}
		result, err := s.db.ExecContext(ctx, query)
		if err != nil {
			return errors.WithStack(err)
		}
		numRows, err := result.RowsAffected()
		if err != nil {
			return errors.WithStack(err)
		}
		if numRows != 1 {
			return errors.Errorf("expected to affect 1 row, affected %d", numRows)
		}
	}
	return nil
}

// QueryPages implements chunk.IndexClient.
func (s *StorageClient) QueryPages(ctx context.Context, queries []chunk.IndexQuery, callback func(chunk.IndexQuery, chunk.ReadBatch) bool) error {
	// A limitation of this approach is that this only fetches whole rows; but
	// whatever, we filter them in the cache on the client.  But for unit tests to
	// pass, we must do this.
	callback = util.QueryFilter(callback)
	for _, query := range queries {
		var builder strings.Builder
		builder.WriteString(fmt.Sprintf(`SELECT table, range, value FROM %s WHERE hash = '%s'`, query.TableName, query.HashValue))

		if len(query.RangeValuePrefix) != 0 {
			builder.WriteString(fmt.Sprintf(` AND range LIKE '%s%%'`, string(query.RangeValuePrefix)))
		} else if len(query.RangeValueStart) != 0 {
			builder.WriteString(fmt.Sprintf(` AND range >= '%s'`, string(query.RangeValueStart)))
		}
		if len(query.ValueEqual) != 0 {
			builder.WriteString(fmt.Sprintf(` AND value == '%s'`, string(query.ValueEqual)))
		}

		rows, err := s.db.QueryContext(ctx, builder.String())
		if err != nil {
			return errors.WithStack(err)
		}

		defer rows.Close()

		rowsBatch := &rowsBatch{rows: rows}
		for _, query := range queries {
			callback(query, rowsBatch)
		}

		// Check for errors from iterating over rows.
		if err := rows.Err(); err != nil {
			return errors.WithStack(err)

		}

	}
	return nil
}

type rowsBatch struct {
	rows *sql.Rows
}

func (b *rowsBatch) Iterator() chunk.ReadBatchIterator {
	return &rowsBatchIterator{
		rows: b.rows,
	}
}

type rowsBatchIterator struct {
	rows       *sql.Rows
	rangeValue []byte
	value      []byte
}

func (b *rowsBatchIterator) Next() bool {
	next := b.rows.Next()
	if !next {
		return false
	}
	if err := b.rows.Scan(&b.rangeValue, &b.value); err != nil {
		// TODO: logging/error handling better
		log.Printf("error scanning row: %s", err)
		return false
	}
	return true
}

func (b *rowsBatchIterator) RangeValue() []byte {
	return b.rangeValue
}

func (b *rowsBatchIterator) Value() []byte {
	return b.value
}
