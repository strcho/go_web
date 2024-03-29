package service

import (
	"context"

	v1 "server/api/helloworld/v1"
	"server/internal/biz"
)

// GreeterService is a greeter service.
type GreeterService struct {
	v1.UnimplementedGreeterServer

	uc *biz.GreeterUsecase
}

// NewGreeterService new a greeter service.
func NewGreeterService(uc *biz.GreeterUsecase) *GreeterService {
	return &GreeterService{uc: uc}
}

// SayHello implements helloworld.GreeterServer.
func (s *GreeterService) SayHello(ctx context.Context, in *v1.HelloRequest) (*v1.HelloReply, error) {
	g, err := s.uc.CreateGreeter(ctx, &biz.Greeter{Hello: in.Name})
	if err != nil {
		return nil, err
	}
	return &v1.HelloReply{Message: "Hello " + g.Hello}, nil
}

func (s *GreeterService) SayOk(ctx context.Context, in *v1.OkRequest) (*v1.OkReply, error) {
	g, err := s.uc.CreateGreeter(ctx, &biz.Greeter{Hello: in.V})
	if err != nil {
		return nil, err
	}
	return &v1.OkReply{Message: "Hello " + g.Hello}, nil
}
