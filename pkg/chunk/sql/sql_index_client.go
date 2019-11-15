package sql

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/util"
)

type Config struct {
	DSN        string
	Driver     string
	IndexTable string
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.DSN, "sql.dsn", "", "DSN for connecting to the database")
	f.StringVar(&cfg.Driver, "sql.driver", "postgresql", "The database driver to use")
	f.StringVar(&cfg.Driver, "sql.indexTable", "cortex_chunk_indices", "")
}

// StorageClient implements chunk.IndexClient for SQL databases.
type StorageClient struct {
	db         *sql.DB
	indexTable string
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

	client := &StorageClient{
		db:         db,
		indexTable: cfg.IndexTable,
	}

	if err := client.createTable(); err != nil {
		return nil, errors.WithStack(err)
	}

	return client, nil
}

func (s *StorageClient) createTable() error {
	_, err := s.db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	table varchar,
	hash varchar,
	range varchar,
	value varchar,
	PRIMARY KEY(table, hash)
	)`, s.indexTable))
	if err != nil {
		return errors.Wrap(err, "create table failed")
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
		result, err := s.db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (table, hash, range, value) VALUES ('%s', '%s', '%s', '%s')",
			s.indexTable, entry.TableName, entry.HashValue, string(entry.RangeValue), string(entry.Value)))
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

	// var comparisons []string
	// for _, indexQuery := range queries {

	// }
	query := fmt.Sprintf(`SELECT range, value FROM %s`, s.indexTable)
	rows, err := s.db.QueryContext(ctx, query)
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
