package tests

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/mightyguava/autotx"
)

func TestTransact(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	mock.ExpectBegin()
	mock.ExpectCommit()
	err = autotx.Transact(context.Background(), db, func(tx *sql.Tx) error {
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTransact_beginErr(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	expectErr := errors.New("boom")
	mock.ExpectBegin().WillReturnError(expectErr)
	err = autotx.Transact(context.Background(), db, func(tx *sql.Tx) error {
		return nil
	})
	require.Error(t, err)
	require.Equal(t, expectErr, errors.Cause(err))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTransact_commitErr(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	expectErr := errors.New("boom")
	mock.ExpectBegin()
	mock.ExpectCommit().WillReturnError(expectErr)
	err = autotx.Transact(context.Background(), db, func(tx *sql.Tx) error {
		return nil
	})
	require.Error(t, err)
	require.Equal(t, expectErr, errors.Cause(err))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTransact_rollbackErrNotRetried(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	rollbackErr := errors.New("boom")
	inTxErr := errors.New("rollback")
	mock.ExpectBegin()
	mock.ExpectRollback().WillReturnError(rollbackErr)
	err = autotx.Transact(context.Background(), db, func(tx *sql.Tx) error {
		return inTxErr
	})
	require.Error(t, err)
	require.IsType(t, &autotx.RollbackErr{}, err)
	require.Equal(t, inTxErr, errors.Cause(err))
	require.Contains(t, err.Error(), rollbackErr.Error())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTransact_panicErr(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	inTxErr := errors.New("in tx panic")
	mock.ExpectBegin()
	mock.ExpectRollback()
	require.Panics(t, func() {
		_ = autotx.Transact(context.Background(), db, func(tx *sql.Tx) error {
			panic(inTxErr)
		})
	})
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTransactWithRetry_TerminatesAfterMaxRetries(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	maxRetries := 3
	// Expect 3 attempts
	for i := 0; i < maxRetries; i++ {
		mock.ExpectBegin()
		mock.ExpectRollback()
	}
	txErr := errors.New("retryable")
	count := 0
	sleepCount := &sleepCounter{}
	err = autotx.TransactWithRetry(context.Background(), db, autotx.RetryOpts{
		MaxRetries: maxRetries,
		Sleep:      sleepCount.Sleep,
	}, func(tx *sql.Tx) error {
		count++
		return txErr
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "retryable")
	require.Equal(t, 3, count)
	require.Equal(t, 3, sleepCount.Count)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTransactWithRetry_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	mock.ExpectBegin()
	mock.ExpectRollback()
	mock.ExpectBegin()
	mock.ExpectCommit()
	retryable := errors.New("retryable")
	count := 0
	err = autotx.TransactWithRetry(context.Background(), db, autotx.RetryOpts{MaxRetries: 3}, func(tx *sql.Tx) error {
		count++
		if count == 1 {
			return retryable
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, count)
	require.NoError(t, mock.ExpectationsWereMet())
}

type sleepCounter struct {
	Count int
}

func (c *sleepCounter) Inc() {
	c.Count++
}

func (c *sleepCounter) Sleep(duration time.Duration) {
	c.Count++
}
