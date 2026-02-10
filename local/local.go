package local

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/geulgyeol/queue/db"
	"github.com/jackc/pgx/v5/pgtype"
)

// QueueService defines the interface for queue operations
type QueueService interface {
	// Content Queue
	PopContentQueueItems(ctx context.Context, arg db.PopContentQueueItemsParams) ([]db.ContentQueue, error)
	EnqueueContentItems(ctx context.Context, payloads []string) (int64, error)
	DeleteContentQueueItems(ctx context.Context, ids []int64) (int64, error)

	// Profile Queue
	PopProfileQueueItems(ctx context.Context, arg db.PopProfileQueueItemsParams) ([]db.ProfileQueue, error)
	EnqueueProfileItems(ctx context.Context, payloads []string) (int64, error)
	DeleteProfileQueueItems(ctx context.Context, ids []int64) (int64, error)

	// User Queue
	PopUserQueueItems(ctx context.Context, arg db.PopUserQueueItemsParams) ([]db.UserQueue, error)
	EnqueueUserItems(ctx context.Context, payloads []string) (int64, error)
	DeleteUserQueueItems(ctx context.Context, ids []int64) (int64, error)
}

// LocalQueries implements in-memory queue for local development/testing
type LocalQueries struct {
	mu           sync.Mutex
	contentQueue []db.ContentQueue
	profileQueue []db.ProfileQueue
	userQueue    []db.UserQueue
	contentIDSeq atomic.Int64
	profileIDSeq atomic.Int64
	userIDSeq    atomic.Int64
}

func New() *LocalQueries {
	return &LocalQueries{
		contentQueue: make([]db.ContentQueue, 0),
		profileQueue: make([]db.ProfileQueue, 0),
		userQueue:    make([]db.UserQueue, 0),
	}
}

// Content Queue Operations

func (q *LocalQueries) PopContentQueueItems(_ context.Context, arg db.PopContentQueueItemsParams) ([]db.ContentQueue, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	var result []db.ContentQueue
	count := 0

	for i := range q.contentQueue {
		if count >= int(arg.Limit) {
			break
		}

		item := &q.contentQueue[i]
		if item.Attempts > arg.Attempts {
			continue
		}

		lockedUntil := time.Time{}
		if item.LockedUntil.Valid {
			lockedUntil = item.LockedUntil.Time
		}

		isWaitingAndUnlocked := item.Status == "waiting" && (!item.LockedUntil.Valid || lockedUntil.Before(now))
		isProcessingAndExpired := item.Status == "processing" && lockedUntil.Before(now)

		if isWaitingAndUnlocked || isProcessingAndExpired {
			item.Status = "processing"
			item.LockedUntil = pgtype.Timestamp{Time: now.Add(120 * time.Second), Valid: true}
			item.Attempts++
			result = append(result, *item)
			count++
		}
	}

	return result, nil
}

func (q *LocalQueries) EnqueueContentItems(_ context.Context, payloads []string) (int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, payload := range payloads {
		id := q.contentIDSeq.Add(1)
		q.contentQueue = append(q.contentQueue, db.ContentQueue{
			ID:         id,
			Payload:    payload,
			EnqueuedAt: pgtype.Timestamp{Time: time.Now(), Valid: true},
			Attempts:   0,
			Status:     "waiting",
		})
	}

	return int64(len(payloads)), nil
}

func (q *LocalQueries) DeleteContentQueueItems(_ context.Context, ids []int64) (int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	idSet := make(map[int64]bool)
	for _, id := range ids {
		idSet[id] = true
	}

	var deleted int64
	newQueue := make([]db.ContentQueue, 0, len(q.contentQueue))
	for _, item := range q.contentQueue {
		if idSet[item.ID] && item.Status == "processing" {
			deleted++
		} else {
			newQueue = append(newQueue, item)
		}
	}
	q.contentQueue = newQueue

	return deleted, nil
}

// Profile Queue Operations

func (q *LocalQueries) PopProfileQueueItems(_ context.Context, arg db.PopProfileQueueItemsParams) ([]db.ProfileQueue, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	var result []db.ProfileQueue
	count := 0

	for i := range q.profileQueue {
		if count >= int(arg.Limit) {
			break
		}

		item := &q.profileQueue[i]
		if item.Attempts > arg.Attempts {
			continue
		}

		lockedUntil := time.Time{}
		if item.LockedUntil.Valid {
			lockedUntil = item.LockedUntil.Time
		}

		isWaitingAndUnlocked := item.Status == "waiting" && (!item.LockedUntil.Valid || lockedUntil.Before(now))
		isProcessingAndExpired := item.Status == "processing" && lockedUntil.Before(now)

		if isWaitingAndUnlocked || isProcessingAndExpired {
			item.Status = "processing"
			item.LockedUntil = pgtype.Timestamp{Time: now.Add(120 * time.Second), Valid: true}
			item.Attempts++
			result = append(result, *item)
			count++
		}
	}

	return result, nil
}

func (q *LocalQueries) EnqueueProfileItems(_ context.Context, payloads []string) (int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, payload := range payloads {
		id := q.profileIDSeq.Add(1)
		q.profileQueue = append(q.profileQueue, db.ProfileQueue{
			ID:         id,
			Payload:    payload,
			EnqueuedAt: pgtype.Timestamp{Time: time.Now(), Valid: true},
			Attempts:   0,
			Status:     "waiting",
		})
	}

	return int64(len(payloads)), nil
}

func (q *LocalQueries) DeleteProfileQueueItems(_ context.Context, ids []int64) (int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	idSet := make(map[int64]bool)
	for _, id := range ids {
		idSet[id] = true
	}

	var deleted int64
	newQueue := make([]db.ProfileQueue, 0, len(q.profileQueue))
	for _, item := range q.profileQueue {
		if idSet[item.ID] && item.Status == "processing" {
			deleted++
		} else {
			newQueue = append(newQueue, item)
		}
	}
	q.profileQueue = newQueue

	return deleted, nil
}

// User Queue Operations

func (q *LocalQueries) PopUserQueueItems(_ context.Context, arg db.PopUserQueueItemsParams) ([]db.UserQueue, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	var result []db.UserQueue
	count := 0

	for i := range q.userQueue {
		if count >= int(arg.Limit) {
			break
		}

		item := &q.userQueue[i]
		if item.Attempts > arg.Attempts {
			continue
		}

		lockedUntil := time.Time{}
		if item.LockedUntil.Valid {
			lockedUntil = item.LockedUntil.Time
		}

		isWaitingAndUnlocked := item.Status == "waiting" && (!item.LockedUntil.Valid || lockedUntil.Before(now))
		isProcessingAndExpired := item.Status == "processing" && lockedUntil.Before(now)

		if isWaitingAndUnlocked || isProcessingAndExpired {
			item.Status = "processing"
			item.LockedUntil = pgtype.Timestamp{Time: now.Add(120 * time.Second), Valid: true}
			item.Attempts++
			result = append(result, *item)
			count++
		}
	}

	return result, nil
}

func (q *LocalQueries) EnqueueUserItems(_ context.Context, payloads []string) (int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, payload := range payloads {
		id := q.userIDSeq.Add(1)
		q.userQueue = append(q.userQueue, db.UserQueue{
			ID:         id,
			Payload:    payload,
			EnqueuedAt: pgtype.Timestamp{Time: time.Now(), Valid: true},
			Attempts:   0,
			Status:     "waiting",
		})
	}

	return int64(len(payloads)), nil
}

func (q *LocalQueries) DeleteUserQueueItems(_ context.Context, ids []int64) (int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	idSet := make(map[int64]bool)
	for _, id := range ids {
		idSet[id] = true
	}

	var deleted int64
	newQueue := make([]db.UserQueue, 0, len(q.userQueue))
	for _, item := range q.userQueue {
		if idSet[item.ID] && item.Status == "processing" {
			deleted++
		} else {
			newQueue = append(newQueue, item)
		}
	}
	q.userQueue = newQueue

	return deleted, nil
}
