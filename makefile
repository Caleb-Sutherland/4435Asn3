compileproto: 
	protoc --go_out=. --go_opt=paths=source_relative \
	--go-grpc_out=require_unimplemented_servers=false:. \
	--go-grpc_opt=paths=source_relative proto/consistenthash.proto

dep:
	go mod init 4435Asn3
	go mod tidy

clean:
	rm go.sum
	rm go.mod	

kill:
	consul kv delete -recurse csuthe
	 	
