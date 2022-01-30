package main

import (
	"bapi/internal/common"
	"bapi/internal/pb"
	"bapi/internal/server"
	"flag"
	"fmt"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	ctx := common.NewBapiCtx()
	port := 50051

	var backfillFile *string = nil
	flagFile := flag.String("backfill_file", "", "file with rows to backfill")
	flag.Parse()

	if *flagFile != "" {
		if _, err := os.Stat(*flagFile); err != nil {
			ctx.Logger.Fatalf("backfill_file does not exist: %s, %v", *flagFile, err)
		}
		backfillFile = flagFile
	}

	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		ctx.Logger.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()

	reflection.Register(s)
	pb.RegisterBapiServer(s, server.NewServer(ctx, backfillFile))

	ctx.Logger.Infof("server listening at %v", lis.Addr())

	if err := s.Serve(lis); err != nil {
		ctx.Logger.Fatalf("failed to serve: %v", err)
	}
}
