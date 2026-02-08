# Retry policy

TaskHarbor retries are built from two pieces:

1) A per-job limit (JobRequest.MaxAttempts)
2) A worker retry policy (RetryPolicy) that decides the delay between attempts

This doc explains how they interact and what the defaults are.

## What MaxAttempts means

JobRequest.MaxAttempts is job-level. It answers:
How many times can this job fail before it is moved to the DLQ?

- Stored with the job (driver record / Postgres max_attempts).
- Travels with the job regardless of which worker processes it.
- Attempts starts at 0 and increments on each failure.

Example:
- MaxAttempts = 5 means the job can fail 4 times and will be DLQ’d on the 5th failure (when Attempts would reach 5).

## What RetryPolicy means

RetryPolicy is worker-level. It answers:
If we are going to retry, how long should we wait before the next attempt?

On each failure, the worker computes:
- nextAttempts = current Attempts + 1
- delay = RetryPolicy.NextDelay(nextAttempts)
- run_at = now + delay
- Attempts = nextAttempts
- last_error/failed_at updated
- lease cleared and job becomes ready again (but only runnable when run_at <= now)

RetryPolicy is not the source of truth for whether a job is allowed to retry. It only provides timing.

## Default behavior

By default, TaskHarbor configures the worker with an exponential backoff retry policy, so jobs do not DLQ on the first failure.

Default retry policy parameters:
- base delay: 200ms
- max delay: 5s
- multiplier: 2.0
- jitter: 0.20
- default max attempts: 25

The default max attempts is used when a job does not specify JobRequest.MaxAttempts.

## How the effective max attempts is chosen

The worker chooses an effective maximum for each job:

- If the job sets MaxAttempts > 0, that value is used.
- If the job does not set MaxAttempts, the worker uses RetryPolicy.MaxAttempts().

Optional global safety cap:
- If both are set (> 0), the effective maximum is:
  min(job MaxAttempts, policy MaxAttempts)

This lets operators cap retries globally even if a producer enqueues jobs with very large MaxAttempts.

## DLQ rules

A job is moved to DLQ when any of the following happens:

1) The handler returns an Unrecoverable error
- Unrecoverable errors skip retries and DLQ immediately.

2) The job reaches its effective max attempts
- nextAttempts >= effectiveMax => DLQ

## Disabling retries

If you want “fail fast” behavior (DLQ on first handler error), configure the worker with no retry policy:

- WithRetryPolicy(nil)

Or set MaxAttempts <= 0 at the job level and also provide a policy with MaxAttempts() <= 0.

## Exponential backoff with jitter

Exponential backoff delay formula:
- delay = base * multiplier^(attempt-1), capped at maxDelay

Jitter:
- choose u uniformly in [1-j, 1+j]
- jittered delay = delay * u

## Examples

### Use the defaults

If you do nothing, the worker uses the default retry policy.

You can still set job-level MaxAttempts:

```go
_, _ = client.Enqueue(ctx, taskharbor.JobRequest{
  Type: "email:send",
  Payload: EmailPayload{...},
  Queue: "default",
  MaxAttempts: 5,
})
```

### Customize retry timing and cap

```go
rp := taskharbor.NewExponentialBackoffPolicy(
  200*time.Millisecond,
  10*time.Second,
  2.0,
  0.10,
  taskharbor.WithMaxAttempts(15),
)

worker := taskharbor.NewWorker(
  drv,
  taskharbor.WithRetryPolicy(rp),
)
```

### Global cap smaller than job cap

If the job asks for 20 attempts but the policy cap is 10:
- effective max attempts = 10
- the job will be DLQ’d once Attempts reaches 10

## Notes for Postgres

- TIMESTAMPTZ stores microsecond precision, so run_at comparisons in tests should use time.Truncate(time.Microsecond) for deterministic equality checks.
- run_at NULL means runnable immediately.
