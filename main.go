package main

import (
	"context"
	"fmt"
	"os"

	"github.com/akamensky/argparse"
	"github.com/geulgyeol/queue/db"
	"github.com/geulgyeol/queue/local"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
)

type QueueItem struct {
	ID      int64  `json:"id"`
	Payload string `json:"payload"`
}

func main() {
	gin.SetMode(gin.ReleaseMode)

	parser := argparse.NewParser("geulgyeol-queue", "A task queue server for Geulgyeol.")

	port := parser.Int("p", "port", &argparse.Options{Default: 8080, Help: "Port to run the server on"})
	maxAttempts := parser.Int("a", "max-attempts", &argparse.Options{Default: 100, Help: "Maximum number of processing attempts before giving up"})
	batchSize := parser.Int("b", "batch-size", &argparse.Options{Default: 100, Help: "Number of items to return in each batch"})
	connString := parser.String("c", "conn-string", &argparse.Options{Help: "PostgreSQL connection string"})
	localMode := parser.Flag("l", "local", &argparse.Options{Help: "Whether to use emulator"})

	err := parser.Parse(os.Args)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	var queries local.QueueService

	if *localMode {
		queries = local.New()
		fmt.Println("Running in local emulation mode (in-memory queue)")
	} else {
		conn, err := pgx.Connect(ctx, *connString)
		if err != nil {
			panic(err)
		}
		defer func(conn *pgx.Conn, ctx context.Context) {
			err := conn.Close(ctx)
			if err != nil {
				fmt.Printf("Error closing database connection: %v\n", err)
			}
		}(conn, ctx)

		queries = db.New(conn)
	}

	r := gin.Default()

	r.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	r.GET("/content", func(c *gin.Context) {
		res, err := queries.PopContentQueueItems(c, db.PopContentQueueItemsParams{Attempts: int32(*maxAttempts), Limit: int32(*batchSize)})

		if err != nil {
			c.JSON(500, gin.H{"error": "Internal server error"})
			fmt.Printf("Error popping content queue items: %v\n", err)
			return
		}

		apiRes := make([]QueueItem, 0, len(res))
		for _, item := range res {
			apiRes = append(apiRes, QueueItem{ID: item.ID, Payload: item.Payload})
		}

		c.JSON(200, apiRes)
	})

	r.POST("/content", func(c *gin.Context) {
		// enqueue
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
		// delete processed items
		var req struct {
			IDs []int64 `json:"ids"`
		}
		if err := c.BindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": "Bad request"})
			return
		}

		if len(req.IDs) == 0 {
			c.JSON(400, gin.H{"error": "No IDs provided"})
			return
		}

		cnt, err := queries.DeleteContentQueueItems(c, req.IDs)
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
		res, err := queries.PopProfileQueueItems(c, db.PopProfileQueueItemsParams{Attempts: int32(*maxAttempts), Limit: int32(*batchSize)})

		if err != nil {
			c.JSON(500, gin.H{"error": "Internal server error"})
			fmt.Printf("Error popping content queue items: %v\n", err)
			return
		}

		apiRes := make([]QueueItem, 0, len(res))
		for _, item := range res {
			apiRes = append(apiRes, QueueItem{ID: item.ID, Payload: item.Payload})
		}

		c.JSON(200, apiRes)
	})

	r.POST("/profile", func(c *gin.Context) {
		// enqueue
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
		// delete processed items
		var req struct {
			IDs []int64 `json:"ids"`
		}
		if err := c.BindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": "Bad request"})
			return
		}

		if len(req.IDs) == 0 {
			c.JSON(400, gin.H{"error": "No IDs provided"})
			return
		}

		cnt, err := queries.DeleteProfileQueueItems(c, req.IDs)
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
		res, err := queries.PopUserQueueItems(c, db.PopUserQueueItemsParams{Attempts: int32(*maxAttempts), Limit: int32(*batchSize)})

		if err != nil {
			c.JSON(500, gin.H{"error": "Internal server error"})
			fmt.Printf("Error popping content queue items: %v\n", err)
			return
		}

		apiRes := make([]QueueItem, 0, len(res))
		for _, item := range res {
			apiRes = append(apiRes, QueueItem{ID: item.ID, Payload: item.Payload})
		}

		c.JSON(200, apiRes)
	})

	r.POST("/user", func(c *gin.Context) {
		// enqueue
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
		// delete processed items
		var req struct {
			IDs []int64 `json:"ids"`
		}

		if err := c.BindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": "Bad request"})
			return
		}

		if len(req.IDs) == 0 {
			c.JSON(400, gin.H{"error": "No IDs provided"})
			return
		}

		cnt, err := queries.DeleteUserQueueItems(c, req.IDs)
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

	fmt.Printf("Starting server on port %d\n", *port)

	// run the server
	_ = r.Run(fmt.Sprintf(":%d", *port))
}
