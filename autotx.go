package autotx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"
)

// DefaultMaxRetries configures the default number of max retries attempted by TransactWithRetry.
var DefaultMaxRetries = 5

// DefaultIsRetryable configures the default function for determining whether the error returned from the operation is
// retryable. By default, all errors are retryable. A RollbackErr is never retryable..
var DefaultIsRetryable = alwaysRetryable

// Transact executes the operation inside a transaction, committing the transaction on operation completion. If the
// operation returns an error or panic, the transaction will automatically be rolled back, returning the original error
// or propagating the original panic. If the rollback caused by an error also receives an error, a RollbackErr will
// be returned. If the rollback caused by a panic also receives an error, the error message and original panic will
// be wrapped inside an error and propagated as a new panic.
func Transact(ctx context.Context, conn *sql.DB, operation func(tx *sql.Tx) error) (err error) {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			if err := tx.Rollback(); err != nil {
				p = errors.New(fmt.Sprintf("panic in transaction, AND rollback failed with error: %v, original panic: %v", err, p))
			}
			panic(p)
		}
		if err != nil {
			if errRollback := tx.Rollback(); errRollback != nil {
				err = &RollbackErr{
					OriginalErr: err,
					Err:         errRollback,
				}
			}
			return
		}
		err = tx.Commit()
	}()
	err = operation(tx)
	return
}

// TransactWithRetry runs the operation using Transact, performing retries according to RetryOpts. If all retries
// fail, the error from the last attempt will be returned. If a rollback fails, no further attempts will be made
// and the RollbackErr will be returned.
//
// Since the transaction operation may be executed multiple times, it is important that any mutations it applies
// to application state (outside the database) be idempotent.
func TransactWithRetry(ctx context.Context, conn *sql.DB, opts RetryOpts, operation func(tx *sql.Tx) error) error {
	if opts.MaxRetries == 0 {
		opts.MaxRetries = DefaultMaxRetries
	}
	if opts.MaxRetries < 0 {
		opts.MaxRetries = math.MaxInt32
	}
	if opts.BackOff == nil {
		opts.BackOff = newSimpleExponentialBackOff().NextBackOff
	}
	if opts.IsRetryable == nil {
		opts.IsRetryable = DefaultIsRetryable
	}
	if opts.Sleep == nil {
		opts.Sleep = time.Sleep
	}
	var err error
	for i := 0; i < opts.MaxRetries; i++ {
		err = Transact(ctx, conn, operation)
		if err == nil {
			return nil
		}
		if !opts.IsRetryable(err) {
			return err
		}
		opts.Sleep(opts.BackOff())
	}
	return err
}

// RollbackErr is the error returned if the transaction operation returned an error, and the rollback automatically
// attempted also returns an error.
type RollbackErr struct {
	// The original error that the operation returned.
	OriginalErr error
	// The error returned by sql.Tx.Rollback()
	Err error
}

// Unwrap returns the OriginalErr.
func (r *RollbackErr) Unwrap() error {
	return r.OriginalErr
}

// Cause returns the OriginalErr.
func (r *RollbackErr) Cause() error {
	return r.Unwrap()
}

// Error returns a formatted error message containing both the OriginalErr and RollbackErr.
func (r *RollbackErr) Error() string {
	return fmt.Sprintf("error rolling back failed transaction: %v, original transaction error: %v", r.Err, r.OriginalErr)
}

// RetryOpts controls how TransactWithRetry behaves.
type RetryOpts struct {
	// MaxRetries configures how many attempts will be made to complete the operation when a retryable error is
	// encountered. The default is DefaultMaxRetries. If set to a negative number, math.MaxInt32 attempts will be made.
	MaxRetries int
	// BackOff controls the backoff behavior. The default backoff is an exponential backoff based on the parameters
	// of DefaultInitialBackOff, DefaultMaxBackOff, and DefaultBackOffFactor. If a negative Duration is returned by
	// NextBackOff(), retries will be aborted.
	BackOff BackOffFunc
	// IsRetryable determines whether the error from the operation should be retried. Return true to retry.
	IsRetryable func(err error) bool
	// Sleep is an optional value to be used for mocking out time.Sleep() for testing. If set, backoff wait
	// will use this function instead of time.Sleep().
	Sleep func(duration time.Duration)
}

func alwaysRetryable(error) bool {
	return true
}
