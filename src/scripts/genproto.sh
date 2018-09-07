#!/usr/bin/env bash
#
# Generate all protobuf bindings. Assumes this script is run from the scripts directory.
# Assumes that the GOPATH is set to the go home directory and that you ran the following command:
#  go get github.com/gogo/protobuf/gogoproto/gogo.proto
set -e
set -u

protoc -I=${GOPATH} -I=${GOPATH}/src/github.com/gogo/protobuf --proto_path=${GOPATH}/src/github.com/gogo/protobuf/gogoproto --java_out=../main/java gogo.proto
protoc -I=${GOPATH} -I=${GOPATH}/src/github.com/gogo/protobuf --proto_path=../protobufs --java_out=../main/java types.proto remote.proto


