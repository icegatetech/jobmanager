package jobmanager

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

type Retrier interface {
	Retry(ctx context.Context, fn func() (needRetry bool, err error)) error
}

type RetrierConfig struct {
	MaxAttempts               int             // Maximum number of attempts.
	ResettingAttemptsDuration time.Duration   // When handler constantly runs inside retry function, need to reset counter after certain time of normal handler operation.
	RandDelay                 time.Duration   // Random delay for less competition between workers.
	Delays                    []time.Duration // Sequence of delays between attempts.
}

const (
	retrierDefaultDelay     = time.Millisecond * 100
	retrierDefaultAttempts  = 20
	retrierDefaultRandDelay = 50 * time.Millisecond
)

var (
	retrierDefaultDelays = []time.Duration{time.Millisecond * 100, time.Millisecond * 100, time.Millisecond * 200, time.Millisecond * 300, time.Millisecond * 700, time.Second, time.Second * 5, time.Second * 10, time.Second * 60}
)

type retrierPolicy func(attempt int) time.Duration

var retrierPolicyDelays = func(randDuration time.Duration, delays []time.Duration) retrierPolicy {
	delayIdx := -1
	return func(attempt int) time.Duration {
		if len(delays) == 0 {
			return retrierDefaultDelay + time.Duration(rand.Int63n(int64(randDuration)))
		}
		delayIdx++
		if delayIdx >= len(delays) {
			return delays[len(delays)-1] + time.Duration(rand.Int63n(int64(randDuration)))
		}
		return delays[delayIdx] + time.Duration(rand.Int63n(int64(randDuration)))
	}
}

type retrier struct {
	lgr Logger
	cfg RetrierConfig
}

func NewRetrier(config RetrierConfig, logger Logger) retrier {
	configDefaults(&config)
	return retrier{cfg: config, lgr: logger}
}

// Retry - controlling result of the callback is needRetry param, the error is simply logged and pushed to the top.
func (r retrier) Retry(ctx context.Context, fn func() (needRetry bool, err error)) error {
	var (
		err                    error
		needRetry              bool
		attemptsReferencePoint = time.Now()
	)
	// NOTICE: counter can be reset.
	for curAttempt := 1; curAttempt <= r.cfg.MaxAttempts; curAttempt++ {
		if ctx.Err() != nil {
			return fmt.Errorf("retry with attempt %d cancelled cause context error: %w", curAttempt, ctx.Err())
		}
		needRetry, err = fn()
		if needRetry {
			if err != nil {
				r.lgr.WarnContext(ctx, fmt.Errorf("retry attempt %d: %w", curAttempt, err).Error())
			} else {
				r.lgr.DebugContext(ctx, fmt.Sprintf("Retry attempt %d", curAttempt))
			}
			// Last time worked for a long time - reset retry counter (start over).
			if r.cfg.ResettingAttemptsDuration > 0 && time.Since(attemptsReferencePoint) > r.cfg.ResettingAttemptsDuration {
				curAttempt = 1
				attemptsReferencePoint = time.Now()
			}
			// TODO(low): replace with timer otherwise we can block goroutine (including on termination)
			time.Sleep(retrierPolicyDelays(r.cfg.RandDelay, r.cfg.Delays)(curAttempt))
			continue
		}
		break
	}

	return err
}

func configDefaults(cfg *RetrierConfig) {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = retrierDefaultAttempts
	}
	if cfg.RandDelay <= 0 {
		cfg.RandDelay = retrierDefaultRandDelay
	}
	if len(cfg.Delays) == 0 {
		cfg.Delays = retrierDefaultDelays
	}
}
