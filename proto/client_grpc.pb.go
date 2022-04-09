// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.14.0
// source: proto/client.proto

package client_proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// ClientReceiveServiceClient is the client API for ClientReceiveService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ClientReceiveServiceClient interface {
	ReceiveFile(ctx context.Context, opts ...grpc.CallOption) (ClientReceiveService_ReceiveFileClient, error)
}

type clientReceiveServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewClientReceiveServiceClient(cc grpc.ClientConnInterface) ClientReceiveServiceClient {
	return &clientReceiveServiceClient{cc}
}

func (c *clientReceiveServiceClient) ReceiveFile(ctx context.Context, opts ...grpc.CallOption) (ClientReceiveService_ReceiveFileClient, error) {
	stream, err := c.cc.NewStream(ctx, &ClientReceiveService_ServiceDesc.Streams[0], "/client.ClientReceiveService/ReceiveFile", opts...)
	if err != nil {
		return nil, err
	}
	x := &clientReceiveServiceReceiveFileClient{stream}
	return x, nil
}

type ClientReceiveService_ReceiveFileClient interface {
	Send(*UploadFileRequest) error
	CloseAndRecv() (*UploadStatus, error)
	grpc.ClientStream
}

type clientReceiveServiceReceiveFileClient struct {
	grpc.ClientStream
}

func (x *clientReceiveServiceReceiveFileClient) Send(m *UploadFileRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *clientReceiveServiceReceiveFileClient) CloseAndRecv() (*UploadStatus, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(UploadStatus)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// ClientReceiveServiceServer is the server API for ClientReceiveService service.
// All implementations should embed UnimplementedClientReceiveServiceServer
// for forward compatibility
type ClientReceiveServiceServer interface {
	ReceiveFile(ClientReceiveService_ReceiveFileServer) error
}

// UnimplementedClientReceiveServiceServer should be embedded to have forward compatible implementations.
type UnimplementedClientReceiveServiceServer struct {
}

func (UnimplementedClientReceiveServiceServer) ReceiveFile(ClientReceiveService_ReceiveFileServer) error {
	return status.Errorf(codes.Unimplemented, "method ReceiveFile not implemented")
}

// UnsafeClientReceiveServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ClientReceiveServiceServer will
// result in compilation errors.
type UnsafeClientReceiveServiceServer interface {
	mustEmbedUnimplementedClientReceiveServiceServer()
}

func RegisterClientReceiveServiceServer(s grpc.ServiceRegistrar, srv ClientReceiveServiceServer) {
	s.RegisterService(&ClientReceiveService_ServiceDesc, srv)
}

func _ClientReceiveService_ReceiveFile_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(ClientReceiveServiceServer).ReceiveFile(&clientReceiveServiceReceiveFileServer{stream})
}

type ClientReceiveService_ReceiveFileServer interface {
	SendAndClose(*UploadStatus) error
	Recv() (*UploadFileRequest, error)
	grpc.ServerStream
}

type clientReceiveServiceReceiveFileServer struct {
	grpc.ServerStream
}

func (x *clientReceiveServiceReceiveFileServer) SendAndClose(m *UploadStatus) error {
	return x.ServerStream.SendMsg(m)
}

func (x *clientReceiveServiceReceiveFileServer) Recv() (*UploadFileRequest, error) {
	m := new(UploadFileRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// ClientReceiveService_ServiceDesc is the grpc.ServiceDesc for ClientReceiveService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ClientReceiveService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "client.ClientReceiveService",
	HandlerType: (*ClientReceiveServiceServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "ReceiveFile",
			Handler:       _ClientReceiveService_ReceiveFile_Handler,
			ClientStreams: true,
		},
	},
	Metadata: "proto/client.proto",
}