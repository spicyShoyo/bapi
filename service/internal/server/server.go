package server

import (
	common "bapi/internal/common"
	pb "bapi/internal/pb"
	"bapi/internal/store"
	"bufio"
	context "context"
	"strings"
)

type server struct {
	pb.UnimplementedBapiServer
	ctx   *common.BapiCtx
	table *store.Table
}

func NewServer(ctx *common.BapiCtx) *server {
	s := &server{}
	s.ctx = ctx
	// TODO: properly set up the table
	s.table = store.NewTable(ctx, "test_table")
	return s
}

func (s *server) Ping(ctx context.Context, in *pb.PingRequest) (*pb.PingReply, error) {
	s.ctx.Logger.Infof("Received: %v", in.GetName())
	message := "Hello " + in.GetName()
	return &pb.PingReply{
		Status:  pb.Status_OK,
		Message: &message,
	}, nil
}

func (s *server) InitiateShutdown(ctx context.Context, in *pb.InitiateShutdownRequest) (*pb.InitiateShutdownReply, error) {
	s.ctx.Logger.Info("Received: InitiateShutdown, reason: %d", in.GetReason())
	return &pb.InitiateShutdownReply{
		Status:  pb.Status_OK,
		Message: nil,
	}, nil
}

func (s *server) IngestRawRows(ctx context.Context, in *pb.IngestRawRowsRequset) (*pb.IngestRawRowsReply, error) {
	if in.StringRows != nil {
		s.table.IngestBuf(
			bufio.NewScanner(strings.NewReader(*in.StringRows)),
		)
	} else if len(in.JsonRows) != 0 {
		s.table.IngestJsonRows(
			in.JsonRows,
		)
	}

	return &pb.IngestRawRowsReply{
		Status:  pb.Status_ACCEPTED,
		Message: nil,
	}, nil
}

func (s *server) QueryRows(ctx context.Context, in *pb.QueryRowsRequest) (*pb.QueryRowsReply, error) {
	s.ctx.Logger.Info(in.Query)
	result, hasValue := s.table.RowsQuery(in.Query)
	if !hasValue {
		return &pb.QueryRowsReply{
			Status:  pb.Status_NO_CONTENT,
			Message: nil,
			Result:  nil,
		}, nil
	}

	return &pb.QueryRowsReply{
		Status:  pb.Status_OK,
		Message: nil,
		Result:  result,
	}, nil
}
