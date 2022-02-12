package main

import (
	"bapi/internal/pb"
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var logger *zap.SugaredLogger
var serverAddr = "127.0.0.1:50051"

func main() {
	zapLogger, _ := zap.NewDevelopment()
	logger = zapLogger.Sugar()

	r := gin.Default()

	// serve the frontend
	r.Static("/assets", "./cmd/webserver/static/assets")
	r.StaticFile("/", "./cmd/webserver/static/index.html")

	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
	})

	g := r.Group("/v1")
	{
		g.GET("/ping", getPing)
		g.POST("/ingest", postIngest)
		g.GET("/queryRows", getQueryRows)
	}

	r.Run(":8080")
}

func getServiceConnection() (*grpc.ClientConn, bool) {
	conn, e := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		logger.DPanicf("fail to connect to service: %v", e)
		return nil, false
	}
	return conn, true
}

func getPing(c *gin.Context) {
	conn, ok := getServiceConnection()
	if !ok {
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	defer conn.Close()
	client := pb.NewBapiClient(conn)

	reply, e := client.Ping(context.Background(), &pb.PingRequest{Name: "webserver"})
	if e != nil {
		logger.Warnf("fail to get service reply: %v", e)
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	c.JSON(http.StatusOK, &reply)
}

func postIngest(c *gin.Context) {
	request := pb.IngestRawRowsRequset{}
	if err := c.BindJSON(&request); err != nil {
		c.AbortWithError(http.StatusBadRequest, err)
		return
	}

	conn, ok := getServiceConnection()
	if !ok {
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	defer conn.Close()
	client := pb.NewBapiClient(conn)

	reply, e := client.IngestRawRows(context.Background(), &request)
	if e != nil {
		logger.Warnf("fail to get service reply: %v", e)
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	c.JSON(http.StatusAccepted, &reply)
}

// This is to workaround that frontend can't send get request with Json body and
// protobuf doesn't paly well with deserializing url param (for not having `form` tag).
// So we just put the query behind a query param `q`, like `?q=<RowsQuery>`.
type getQueryRowsQuery struct {
	Q pb.RowsQuery `form:"q"`
}

func getQueryRows(c *gin.Context) {
	request := getQueryRowsQuery{}
	//	allow passing as Json body (for testing locally) or url params
	if err := c.ShouldBindJSON(&request); err != nil {
		if err := c.ShouldBindQuery(&request); err != nil {
			c.AbortWithError(http.StatusBadRequest, err)
			return
		}
	}

	conn, ok := getServiceConnection()
	if !ok {
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	defer conn.Close()
	client := pb.NewBapiClient(conn)

	reply, e := client.QueryRows(context.Background(), &request.Q)
	if e != nil {
		logger.Warnf("fail to get service reply: %v", e)
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	c.JSON(http.StatusOK, &reply)
}
