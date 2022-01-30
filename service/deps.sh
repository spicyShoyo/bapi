#!/bin/bash
# generates protobuf code in the local repo so that language server works
go mod tidy; bazel run gen; bazel run deps; 