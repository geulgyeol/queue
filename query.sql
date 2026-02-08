-- name: PopContentQueueItems :many
WITH cte AS (
    SELECT id
    FROM content_queue
    WHERE content_queue.attempts <= $1
      AND (
        (status = 'waiting' AND (locked_until IS NULL OR locked_until < now()))
        OR (status = 'processing' AND locked_until < now())
      )
    ORDER BY enqueued_at
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE content_queue q
SET status = 'processing',
    locked_until = now() + interval '120 seconds',
    attempts = attempts + 1
FROM cte
WHERE q.id = cte.id
    RETURNING q.*;

-- name: PopProfileQueueItems :many
WITH cte AS (
    SELECT id
    FROM profile_queue
    WHERE profile_queue.attempts <= $1
      AND (
        (status = 'waiting' AND (locked_until IS NULL OR locked_until < now()))
        OR (status = 'processing' AND locked_until < now())
      )
    ORDER BY enqueued_at
    LIMIT $2
    FOR UPDATE SKIP LOCKED
            )
UPDATE profile_queue q
SET status = 'processing',
    locked_until = now() + interval '120 seconds',
    attempts = attempts + 1
FROM cte
WHERE q.id = cte.id
    RETURNING q.*;

-- name: PopUserQueueItems :many
WITH cte AS (
    SELECT id
    FROM user_queue
    WHERE user_queue.attempts <= $1
      AND (
        (status = 'waiting' AND (locked_until IS NULL OR locked_until < now()))
        OR (status = 'processing' AND locked_until < now())
      )
    ORDER BY enqueued_at
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE user_queue q
SET status = 'processing',
    locked_until = now() + interval '120 seconds',
    attempts = attempts + 1
FROM cte
WHERE q.id = cte.id
    RETURNING q.*;

-- name: EnqueueContentItem :one
INSERT INTO content_queue (payload)
VALUES ($1)
RETURNING *;