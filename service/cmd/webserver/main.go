package main

import (
	"bapi/internal/pb"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	cors "github.com/rs/cors/wrapper/gin"
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
	r.Use(cors.AllowAll())

	staticResource := os.Getenv("STATIC_RESOURCE")
	if len(staticResource) == 0 {
		staticResource = "./cmd/webserver/static"
	}
	// serve the frontend
	r.Static("/assets", fmt.Sprintf("%s/assets", staticResource))
	r.StaticFile("/", fmt.Sprintf("%s/index.html", staticResource))
	fmt.Printf("%s/assets", staticResource)
	files, err := ioutil.ReadDir(staticResource)
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {
		fmt.Println(f.Name())
	}

	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
	})

	g := r.Group("/v1")
	{
		g.GET("/ping", getPing)
		g.POST("/ingest", postIngest)
		g.GET("/rows", runRowsQuery)
		g.GET("/table", runTableQuery)
		g.GET("/timeline", runTimelineQuery)
		g.GET("/table_info", getTableInfo)
		g.GET("/string_values", searchStrValues)
	}

	port := os.Getenv("PORT")
	if len(port) == 0 {
		port = "8080"
	}
	r.Run(fmt.Sprintf(":%s", port))
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
type rowsQueryWrapper struct {
	Q pb.RowsQuery `form:"q"`
}

type tableQueryWrapper struct {
	Q pb.TableQuery `form:"q"`
}

type timelineQueryWrapper struct {
	Q pb.TimelineQuery `form:"q"`
}

func runRowsQuery(c *gin.Context) {
	request := rowsQueryWrapper{}
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

	reply, e := client.RunRowsQuery(context.Background(), &request.Q)
	if e != nil {
		logger.Warnf("fail to get service reply: %v", e)
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	c.JSON(http.StatusOK, &reply)
}

func runTableQuery(c *gin.Context) {
	request := tableQueryWrapper{}
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

	reply, e := client.RunTableQuery(context.Background(), &request.Q)
	if e != nil {
		logger.Warnf("fail to get service reply: %v", e)
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	c.JSON(http.StatusOK, &reply)
}

func runTimelineQuery(c *gin.Context) {
	request := timelineQueryWrapper{}
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

	reply, e := client.RunTimelineQuery(context.Background(), &request.Q)
	if e != nil {
		logger.Warnf("fail to get service reply: %v", e)
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	c.JSON(http.StatusOK, &reply)
}

func getTableInfo(c *gin.Context) {
	tableName, ok := getSingleParam(c, "table")
	if !ok {
		c.AbortWithStatus(http.StatusBadRequest)
	}

	conn, ok := getServiceConnection()
	if !ok {
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	defer conn.Close()
	client := pb.NewBapiClient(conn)

	reply, e := client.GetTableInfo(context.Background(), &pb.GetTableInfoRequest{
		TableName: tableName,
	})

	if e != nil {
		logger.Warnf("fail to get service reply: %v", e)
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	c.JSON(http.StatusOK, &reply)
}

func searchStrValues(c *gin.Context) {
	tableName, ok1 := getSingleParam(c, "table")
	columnName, ok2 := getSingleParam(c, "column")
	searchString, ok3 := getSingleParam(c, "search_string")
	if !ok1 || !ok2 || !ok3 {
		c.AbortWithStatus(http.StatusBadRequest)
	}

	conn, ok := getServiceConnection()
	if !ok {
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	defer conn.Close()
	client := pb.NewBapiClient(conn)

	reply, e := client.SearchStrValues(context.Background(), &pb.SearchStrValuesRequest{
		TableName:    tableName,
		ColumnName:   columnName,
		SearchString: searchString,
	})

	if e != nil {
		logger.Warnf("fail to get service reply: %v", e)
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	c.JSON(http.StatusOK, &reply)
}

func getSingleParam(c *gin.Context, param string) (string, bool) {
	vals, ok := c.Request.URL.Query()[param]
	if !ok || len(vals) != 1 {
		return "", false
	}

	return vals[0], true
}
