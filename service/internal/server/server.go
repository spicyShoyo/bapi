package server

import (
	common "bapi/internal/common"
	pb "bapi/internal/pb"
	"bapi/internal/store"
	context "context"
)

type server struct {
	pb.UnimplementedBapiServer
	ctx   *common.BapiCtx
	table *store.Table
}

func NewServer(ctx *common.BapiCtx, backfillFile *string) *server {
	s := &server{}
	s.ctx = ctx
	// TODO: properly set up the table
	s.table = store.NewTable(ctx, "test_table")

	if backfillFile != nil {
		s.table.IngestFile(*backfillFile)
	}

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
	s.table.IngestJsonRows(
		in.Rows,
		in.UseServerTs,
	)

	return &pb.IngestRawRowsReply{
		Status:  pb.Status_ACCEPTED,
		Message: nil,
	}, nil
}

func (s *server) RunRowsQuery(ctx context.Context, in *pb.RowsQuery) (*pb.RowsQueryReply, error) {
	s.ctx.Logger.Info(in)
	result, hasValue := s.table.RowsQuery(in)
	if !hasValue {
		return &pb.RowsQueryReply{
			Status:  pb.Status_NO_CONTENT,
			Message: nil,
			Result:  nil,
		}, nil
	}

	return &pb.RowsQueryReply{
		Status:  pb.Status_OK,
		Message: nil,
		Result:  result,
	}, nil
}

func (s *server) RunTableQuery(ctx context.Context, in *pb.TableQuery) (*pb.TableQueryReply, error) {
	s.ctx.Logger.Info(in)
	result, hasValue := s.table.TableQuery(in)
	if !hasValue {
		return &pb.TableQueryReply{
			Status:  pb.Status_NO_CONTENT,
			Message: nil,
			Result:  nil,
		}, nil
	}

	return &pb.TableQueryReply{
		Status:  pb.Status_OK,
		Message: nil,
		Result:  result,
	}, nil
}

func (s *server) RunTimelineQuery(ctx context.Context, in *pb.TimelineQuery) (*pb.TimelineQueryReply, error) {
	s.ctx.Logger.Info(in)
	result, hasValue := s.table.TimeilneQuery(in)
	if !hasValue {
		return &pb.TimelineQueryReply{
			Status:  pb.Status_NO_CONTENT,
			Message: nil,
			Result:  nil,
		}, nil
	}

	return &pb.TimelineQueryReply{
		Status:  pb.Status_OK,
		Message: nil,
		Result:  result,
	}, nil
}

func (s *server) GetTableInfo(ctx context.Context, in *pb.GetTableInfoRequest) (*pb.GetTableInfoReply, error) {
	s.ctx.Logger.Info(in)
	tableInfo := s.table.GetTableInfo()
	if tableInfo.TableName == in.TableName {
		return &pb.GetTableInfoReply{
			Status:    pb.Status_OK,
			TableInfo: tableInfo,
		}, nil
	}

	return &pb.GetTableInfoReply{
		Status: pb.Status_NO_CONTENT,
	}, nil
}

func (s *server) GetStrColumnValues(ctx context.Context, in *pb.GetStrColumnValuesRequest) (*pb.GetStrColumnValuesReply, error) {
	return &pb.GetStrColumnValuesReply{}, nil
}
