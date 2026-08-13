.PHONY: proto
proto:
	protoc \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		--go_opt=Mproto/cascade.proto=cascade/gen/cascadepb \
		--go-grpc_opt=Mproto/cascade.proto=cascade/gen/cascadepb \
		proto/cascade.proto
	mkdir -p gen/cascadepb
	mv proto/cascade.pb.go proto/cascade_grpc.pb.go gen/cascadepb/