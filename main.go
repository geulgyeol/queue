package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/akamensky/argparse"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/geulgyeol/queue/db"
	"github.com/geulgyeol/queue/local"
	"github.com/gin-gonic/gin"
)

const visibilityTimeout = 120 * time.Second

type QueueItem struct {
	ID      string `json:"id"`
	Payload string `json:"payload"`
}

type inflightMessage struct {
	messageID pulsar.MessageID
	received  time.Time
}

type TopicQueue struct {
	producer pulsar.Producer
	consumer pulsar.Consumer

	mu       sync.Mutex
	inflight map[string]inflightMessage

	cancel context.CancelFunc
	done   chan struct{}
}

func NewTopicQueue(client pulsar.Client, topic string, subscription string) (*TopicQueue, error) {
	producer, err := client.CreateProducer(pulsar.ProducerOptions{Topic: topic})
	if err != nil {
		return nil, err
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:               topic,
		SubscriptionName:    subscription,
		Type:                pulsar.Shared,
		ReceiverQueueSize:   1,
		NackRedeliveryDelay: 5 * time.Second,
	})
	if err != nil {
		producer.Close()
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	queue := &TopicQueue{
		producer: producer,
		consumer: consumer,
		inflight: make(map[string]inflightMessage),
		cancel:   cancel,
		done:     make(chan struct{}),
	}

	go queue.requeueExpiredLoop(ctx)

	return queue, nil
}

func (q *TopicQueue) Close() {
	q.cancel()
	<-q.done
	q.consumer.Close()
	q.producer.Close()
}

func (q *TopicQueue) Enqueue(ctx context.Context, payloads []string) (int64, error) {
	for _, payload := range payloads {
		_, err := q.producer.Send(ctx, &pulsar.ProducerMessage{Payload: []byte(payload)})
		if err != nil {
			return 0, err
		}
	}

	return int64(len(payloads)), nil
}

func (q *TopicQueue) Pop(ctx context.Context, limit int) ([]QueueItem, error) {
	items := make([]QueueItem, 0, limit)

	for len(items) < limit {
		receiveCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		msg, err := q.consumer.Receive(receiveCtx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				break
			}
			return nil, err
		}

		messageID := msg.ID()
		token := encodeMessageID(messageID)

		q.mu.Lock()
		q.inflight[token] = inflightMessage{messageID: messageID, received: time.Now()}
		q.mu.Unlock()

		items = append(items, QueueItem{ID: token, Payload: string(msg.Payload())})
	}

	return items, nil
}

func (q *TopicQueue) Delete(ids []string) (int64, error) {
	var deleted int64

	for _, token := range ids {
		messageID, err := decodeMessageID(token)
		if err != nil {
			continue
		}

		if err := q.consumer.AckID(messageID); err != nil {
			return deleted, err
		}

		q.mu.Lock()
		delete(q.inflight, token)
		q.mu.Unlock()
		deleted++
	}

	return deleted, nil
}

func (q *TopicQueue) requeueExpiredLoop(ctx context.Context) {
	defer close(q.done)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			q.requeueExpired()
		}
	}
}

func (q *TopicQueue) requeueExpired() {
	now := time.Now()
	expired := make([]pulsar.MessageID, 0)
	expiredTokens := make([]string, 0)

	q.mu.Lock()
	for token, item := range q.inflight {
		if now.Sub(item.received) < visibilityTimeout {
			continue
		}
		expired = append(expired, item.messageID)
		expiredTokens = append(expiredTokens, token)
	}
	for _, token := range expiredTokens {
		delete(q.inflight, token)
	}
	q.mu.Unlock()

	for _, messageID := range expired {
		q.consumer.NackID(messageID)
	}
}

type QueueService struct {
	content *TopicQueue
	profile *TopicQueue
	user    *TopicQueue
}

func NewQueueService(client pulsar.Client) (*QueueService, error) {
	content, err := NewTopicQueue(client, "content", "content-sub")
	if err != nil {
		return nil, err
	}

	profile, err := NewTopicQueue(client, "profile", "profile-sub")
	if err != nil {
		content.Close()
		return nil, err
	}

	user, err := NewTopicQueue(client, "user", "user-sub")
	if err != nil {
		profile.Close()
		content.Close()
		return nil, err
	}

	return &QueueService{content: content, profile: profile, user: user}, nil
}

func (q *QueueService) Close() {
	q.content.Close()
	q.profile.Close()
	q.user.Close()
}

func (q *QueueService) ForTopic(topic string) *TopicQueue {
	switch topic {
	case "content":
		return q.content
	case "profile":
		return q.profile
	default:
		return q.user
	}
}

func registerTopicRoutes(r *gin.Engine, topic string, queue *TopicQueue, batchSize int) {
	r.GET("/"+topic, func(c *gin.Context) {
		res, err := queue.Pop(c, batchSize)
		if err != nil {
			c.JSON(500, gin.H{"error": "Internal server error"})
			fmt.Printf("Error popping %s queue items: %v\n", topic, err)
			return
		}

		if len(res) == 0 {
			c.Status(204)
			return
		}

		c.JSON(200, res)
	})

	r.POST("/"+topic, func(c *gin.Context) {
		var req struct {
			Payloads []string `json:"payloads"`
		}
		if err := c.BindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": "Bad request"})
			return
		}

		if len(req.Payloads) == 0 {
			c.JSON(400, gin.H{"error": "No payloads provided"})
			return
		}

		_, err := queue.Enqueue(c, req.Payloads)
		if err != nil {
			c.JSON(500, gin.H{"error": "Internal server error"})
			fmt.Printf("Error enqueuing %s items: %v\n", topic, err)
			return
		}

		c.JSON(200, gin.H{"status": "enqueued"})
	})

	r.DELETE("/"+topic, func(c *gin.Context) {
		var req struct {
			IDs []string `json:"ids"`
		}

		if err := c.BindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": "Bad request"})
			return
		}

		if len(req.IDs) == 0 {
			c.JSON(400, gin.H{"error": "No IDs provided"})
			return
		}

		cnt, err := queue.Delete(req.IDs)
		if err != nil {
			c.JSON(500, gin.H{"error": "Internal server error"})
			fmt.Printf("Error deleting %s items: %v\n", topic, err)
			return
		}

		if cnt == 0 {
			c.JSON(400, gin.H{"error": "No items deleted"})
			return
		}

		c.JSON(200, gin.H{"status": "deleted"})
	})
}

func registerLocalRoutes(r *gin.Engine, queries local.QueueService, maxAttempts int, batchSize int) {
	r.GET("/content", func(c *gin.Context) {
		res, err := queries.PopContentQueueItems(c, db.PopContentQueueItemsParams{Attempts: int32(maxAttempts), Limit: int32(batchSize)})
		if err != nil {
			c.JSON(500, gin.H{"error": "Internal server error"})
			fmt.Printf("Error popping content queue items: %v\n", err)
			return
		}

		apiRes := make([]QueueItem, 0, len(res))
		for _, item := range res {
			apiRes = append(apiRes, QueueItem{ID: strconv.FormatInt(item.ID, 10), Payload: item.Payload})
		}

		if len(apiRes) == 0 {
			c.Status(204)
			return
		}

		c.JSON(200, apiRes)
	})

	r.POST("/content", func(c *gin.Context) {
		var req struct {
			Payloads []string `json:"payloads"`
		}
		if err := c.BindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": "Bad request"})
			return
		}

		if len(req.Payloads) == 0 {
			c.JSON(400, gin.H{"error": "No payloads provided"})
			return
		}

		_, err := queries.EnqueueContentItems(c, req.Payloads)
		if err != nil {
			c.JSON(500, gin.H{"error": "Internal server error"})
			fmt.Printf("Error enqueuing content items: %v\n", err)
			return
		}

		c.JSON(200, gin.H{"status": "enqueued"})
	})

	r.DELETE("/content", func(c *gin.Context) {
		var req struct {
			IDs []string `json:"ids"`
		}
		if err := c.BindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": "Bad request"})
			return
		}

		if len(req.IDs) == 0 {
			c.JSON(400, gin.H{"error": "No IDs provided"})
			return
		}

		ids, err := parseIntIDs(req.IDs)
		if err != nil {
			c.JSON(400, gin.H{"error": "Invalid IDs"})
			return
		}

		cnt, err := queries.DeleteContentQueueItems(c, ids)
		if err != nil {
			c.JSON(500, gin.H{"error": "Internal server error"})
			fmt.Printf("Error deleting content queue items: %v\n", err)
			return
		}

		if cnt == 0 {
			c.JSON(400, gin.H{"error": "No items deleted"})
			return
		}

		c.JSON(200, gin.H{"status": "deleted"})
	})

	r.GET("/profile", func(c *gin.Context) {
		res, err := queries.PopProfileQueueItems(c, db.PopProfileQueueItemsParams{Attempts: int32(maxAttempts), Limit: int32(batchSize)})
		if err != nil {
			c.JSON(500, gin.H{"error": "Internal server error"})
			fmt.Printf("Error popping profile queue items: %v\n", err)
			return
		}

		apiRes := make([]QueueItem, 0, len(res))
		for _, item := range res {
			apiRes = append(apiRes, QueueItem{ID: strconv.FormatInt(item.ID, 10), Payload: item.Payload})
		}

		if len(apiRes) == 0 {
			c.Status(204)
			return
		}

		c.JSON(200, apiRes)
	})

	r.POST("/profile", func(c *gin.Context) {
		var req struct {
			Payloads []string `json:"payloads"`
		}
		if err := c.BindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": "Bad request"})
			return
		}

		if len(req.Payloads) == 0 {
			c.JSON(400, gin.H{"error": "No payloads provided"})
			return
		}

		_, err := queries.EnqueueProfileItems(c, req.Payloads)
		if err != nil {
			c.JSON(500, gin.H{"error": "Internal server error"})
			fmt.Printf("Error enqueuing profile items: %v\n", err)
			return
		}

		c.JSON(200, gin.H{"status": "enqueued"})
	})

	r.DELETE("/profile", func(c *gin.Context) {
		var req struct {
			IDs []string `json:"ids"`
		}
		if err := c.BindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": "Bad request"})
			return
		}

		if len(req.IDs) == 0 {
			c.JSON(400, gin.H{"error": "No IDs provided"})
			return
		}

		ids, err := parseIntIDs(req.IDs)
		if err != nil {
			c.JSON(400, gin.H{"error": "Invalid IDs"})
			return
		}

		cnt, err := queries.DeleteProfileQueueItems(c, ids)
		if err != nil {
			c.JSON(500, gin.H{"error": "Internal server error"})
			fmt.Printf("Error deleting profile queue items: %v\n", err)
			return
		}

		if cnt == 0 {
			c.JSON(400, gin.H{"error": "No items deleted"})
			return
		}

		c.JSON(200, gin.H{"status": "deleted"})
	})

	r.GET("/user", func(c *gin.Context) {
		res, err := queries.PopUserQueueItems(c, db.PopUserQueueItemsParams{Attempts: int32(maxAttempts), Limit: int32(batchSize)})
		if err != nil {
			c.JSON(500, gin.H{"error": "Internal server error"})
			fmt.Printf("Error popping user queue items: %v\n", err)
			return
		}

		apiRes := make([]QueueItem, 0, len(res))
		for _, item := range res {
			apiRes = append(apiRes, QueueItem{ID: strconv.FormatInt(item.ID, 10), Payload: item.Payload})
		}

		if len(apiRes) == 0 {
			c.Status(204)
			return
		}

		c.JSON(200, apiRes)
	})

	r.POST("/user", func(c *gin.Context) {
		var req struct {
			Payloads []string `json:"payloads"`
		}
		if err := c.BindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": "Bad request"})
			return
		}

		if len(req.Payloads) == 0 {
			c.JSON(400, gin.H{"error": "No payloads provided"})
			return
		}

		_, err := queries.EnqueueUserItems(c, req.Payloads)
		if err != nil {
			c.JSON(500, gin.H{"error": "Internal server error"})
			fmt.Printf("Error enqueuing user items: %v\n", err)
			return
		}

		c.JSON(200, gin.H{"status": "enqueued"})
	})

	r.DELETE("/user", func(c *gin.Context) {
		var req struct {
			IDs []string `json:"ids"`
		}
		if err := c.BindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": "Bad request"})
			return
		}

		if len(req.IDs) == 0 {
			c.JSON(400, gin.H{"error": "No IDs provided"})
			return
		}

		ids, err := parseIntIDs(req.IDs)
		if err != nil {
			c.JSON(400, gin.H{"error": "Invalid IDs"})
			return
		}

		cnt, err := queries.DeleteUserQueueItems(c, ids)
		if err != nil {
			c.JSON(500, gin.H{"error": "Internal server error"})
			fmt.Printf("Error deleting user queue items: %v\n", err)
			return
		}

		if cnt == 0 {
			c.JSON(400, gin.H{"error": "No items deleted"})
			return
		}

		c.JSON(200, gin.H{"status": "deleted"})
	})
}

func parseIntIDs(ids []string) ([]int64, error) {
	parsed := make([]int64, 0, len(ids))
	for _, id := range ids {
		num, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			return nil, err
		}
		parsed = append(parsed, num)
	}

	return parsed, nil
}

func encodeMessageID(messageID pulsar.MessageID) string {
	return base64.RawURLEncoding.EncodeToString(messageID.Serialize())
}

func decodeMessageID(token string) (pulsar.MessageID, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, err
	}

	return pulsar.DeserializeMessageID(raw)
}

func main() {
	gin.SetMode(gin.ReleaseMode)

	parser := argparse.NewParser("geulgyeol-queue", "A task queue server for Geulgyeol.")

	port := parser.Int("p", "port", &argparse.Options{Default: 8080, Help: "Port to run the server on"})
	maxAttempts := parser.Int("a", "max-attempts", &argparse.Options{Default: 100, Help: "Maximum number of processing attempts before giving up"})
	batchSize := parser.Int("b", "batch-size", &argparse.Options{Default: 100, Help: "Number of items to return in each batch"})
	pulsarURL := parser.String("u", "pulsar-url", &argparse.Options{Default: "pulsar://10.42.2.65:6650", Help: "Apache Pulsar broker URL"})
	localMode := parser.Flag("l", "local", &argparse.Options{Help: "Whether to use emulator"})

	err := parser.Parse(os.Args)
	if err != nil {
		panic(err)
	}

	r := gin.Default()

	r.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	if *localMode {
		fmt.Println("Running in local emulation mode (in-memory queue)")
		queries := local.New()
		registerLocalRoutes(r, queries, *maxAttempts, *batchSize)
	} else {
		client, err := pulsar.NewClient(pulsar.ClientOptions{URL: *pulsarURL})
		if err != nil {
			panic(err)
		}
		defer client.Close()

		queues, err := NewQueueService(client)
		if err != nil {
			panic(err)
		}
		defer queues.Close()

		registerTopicRoutes(r, "content", queues.ForTopic("content"), *batchSize)
		registerTopicRoutes(r, "profile", queues.ForTopic("profile"), *batchSize)
		registerTopicRoutes(r, "user", queues.ForTopic("user"), *batchSize)
	}

	fmt.Printf("Starting server on port %d\n", *port)

	_ = r.Run(fmt.Sprintf(":%d", *port))
}
