#!/usr/bin/env bash

# executar em vox-core/

uv run python -m grpc_tools.protoc \
  -I ../grpc/protos \
  --python_out=./src/transport/grpc/generated \
  --grpc_python_out=./src/transport/grpc/generated \
  --mypy_out=./src/transport/grpc/generated \
  --mypy_grpc_out=./src/transport/grpc/generated \
  ../grpc/protos/core_worker_internal_communication.proto
