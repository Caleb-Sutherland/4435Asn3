syntax = "proto3";
package consistenthash;


option go_package = "proto/consistenthash.proto;consistenthash_proto";

service ConsistentHash {
    rpc TestConnection (TestRequest) returns (GenericResponse) {}
    // rpc ComputeFingerTable (Empty) returns (GenericResponse) {}
    rpc AddFile (stream UploadFileRequest) returns (UploadStatus) {}
    rpc NotifyNewNodeAdded (NodeInfo) returns (NodeInfoResponse) {}
}

// Testing connection
message TestRequest {
    string message = 1;
}

message GenericResponse {
    string message = 1;
}

message Empty {

}

// Addfile
message CustomFile {
    bytes contents = 1;
    string filename = 2;
}

message UploadStatus {
    string Message = 1;
}

message UploadFileRequest {
    oneof data {
        string filename = 1;
        bytes chunk_data = 2;
    }
}

// Notify new node added to network
message NodeInfo {
    int64 position = 1;
    string address = 2;
}

message NodeInfoResponse {
    bool isSucc = 1;
    bool isPred = 2;
    int64 position = 3;
}

