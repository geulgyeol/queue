package main

import (
	"context"
	"fmt"
	"os"

	"github.com/akamensky/argparse"
	"github.com/geulgyeol/queue/db"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
)

func main() {
	gin.SetMode(gin.ReleaseMode)

	parser := argparse.NewParser("geulgyeol-queue", "A task queue server for Geulgyeol.")

	port := parser.Int("p", "port", &argparse.Options{Default: 8080, Help: "Port to run the server on"})
	maxAttempts := parser.Int("a", "max-attempts", &argparse.Options{Default: 100, Help: "Maximum number of processing attempts before giving up"})
	batchSize := parser.Int("b", "batch-size", &argparse.Options{Default: 100, Help: "Number of items to return in each batch"})
	connString := parser.String("c", "conn-string", &argparse.Options{Help: "PostgreSQL connection string"})

	err := parser.Parse(os.Args)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

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

	queries := db.New(conn)

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

		c.JSON(200, res)
	})

	r.GET("/profile", func(c *gin.Context) {
		res, err := queries.PopProfileQueueItems(c, db.PopProfileQueueItemsParams{Attempts: int32(*maxAttempts), Limit: int32(*batchSize)})

		if err != nil {
			c.JSON(500, gin.H{"error": "Internal server error"})
			fmt.Printf("Error popping content queue items: %v\n", err)
			return
		}

		c.JSON(200, res)
	})

	r.GET("/user", func(c *gin.Context) {
		res, err := queries.PopUserQueueItems(c, db.PopUserQueueItemsParams{Attempts: int32(*maxAttempts), Limit: int32(*batchSize)})

		if err != nil {
			c.JSON(500, gin.H{"error": "Internal server error"})
			fmt.Printf("Error popping content queue items: %v\n", err)
			return
		}

		c.JSON(200, res)
	})

	fmt.Printf("Starting server on port %d\n", *port)

	// run the server
	_ = r.Run(fmt.Sprintf(":%d", *port))
}
