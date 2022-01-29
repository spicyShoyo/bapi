package main

import (
	"bapi/internal/common"
	"bapi/internal/pb"
	"bapi/internal/server"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func runServer() {
	ctx := common.NewBapiCtx()
	port := 50051

	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		ctx.Logger.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()

	reflection.Register(s)
	pb.RegisterBapiServer(s, server.NewServer(ctx))

	ctx.Logger.Infof("server listening at %v", lis.Addr())

	if err := s.Serve(lis); err != nil {
		ctx.Logger.Fatalf("failed to serve: %v", err)
	}
}

func main() {
	runServer()
}
