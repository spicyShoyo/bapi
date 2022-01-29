#!/bin/bash
# generates protobuf code in the local repo so that language server works
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative bapi.proto