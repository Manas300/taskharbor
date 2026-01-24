# Driver Contract

This document defines what a TaskHarbor driver must implement, and what the
core runtime (client/worker) is responsible for.

Principle

- Drivers provide storage + primitive state transitions.
- Core provides semantics (retries, backoff, timeouts, panic recovery, leases later).
- Drivers should stay dumb so new backends can be contributed safely.

## Job record

At minimum, a driver persists these fields:

- id (string)
- type (string)
- queue (string)
- payload (bytes)
- run_at (time): earliest time the job is eligible to run
- timeout (duration)
- attempts (int): number of recorded failures so far
- max_attempts (int): maximum total executions allowed (0 means unset; core may default)
- last_error (string)
- failed_at (time)

## States (v0, milestone 2/3)

These are conceptual; drivers may store them however they want.

- ready: runnable now (run_at is zero or <= now)
- scheduled: run_at is in the future
- inflight: reserved by a worker, awaiting ack / retry / fail
- dlq: dead-lettered jobs

No leases yet in v0 (milestone 4 adds leases + reclaim).

## Interface (v0, milestone 3)

Enqueue

- Store a job in ready or scheduled state depending on run_at.

Reserve

- Return one runnable job for a queue, and move it to inflight.
- Non-blocking: ok=false means nothing runnable.
- Must never return a job that is already inflight.

Ack

- Mark an inflight job successful (terminal).

Retry

- Move an inflight job back to ready/scheduled.
- Persist failure metadata (attempts, last_error, failed_at) and the new run_at.
- Core decides when to retry and whether to DLQ. Drivers only persist.

Fail

- Move an inflight job to DLQ (terminal).
- Core decides when Fail should be used (max attempts exceeded or unrecoverable).

## Error expectations

Drivers should return:

- ErrJobNotFound when an id does not exist
- ErrJobNotInflight when Ack/Retry/Fail is called on a job that is not inflight

## Future milestones

Milestone 4 (leases)

- Reserve returns a lease token + expiry.
- Ack/Retry/Fail require a valid token.
- Expired leases are reclaimed and jobs become reservable again.

Milestone 6 (idempotency)

- Enqueue supports idempotency_key dedupe behavior.

Milestone 8 (CLI)

- Inspection and DLQ management APIs.
