package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc/reflection"

	"route-graph-service/internal/repo"
	"route-graph-service/internal/server"
	pb "route-graph-service/proto/routegraph"

	"google.golang.org/grpc"
)

func main() {
	uri := os.Getenv("NEO4J_URI")
	if uri == "" {
		uri = "neo4j://localhost:7687"
	}
	user := os.Getenv("NEO4J_USER")
	if user == "" {
		user = "neo4j"
	}
	pass := os.Getenv("NEO4J_PASS")
	if pass == "" {
		pass = "test1234"
	}

	r, err := repo.New(uri, user, pass)
	if err != nil {
		log.Fatal("neo4j connect:", err)
	}
	ctx := context.Background()
	defer r.Close(ctx)

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal(err)
	}

	srv := grpc.NewServer()
	s := server.NewServer(r)
	pb.RegisterRouteGraphServer(srv, s)

	reflection.Register(srv)

	go func() {
		log.Println("gRPC server listening on :50051")
		if err := srv.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()

	// wait for shutdown
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	log.Println("shutting down")
	srv.GracefulStop()
}
