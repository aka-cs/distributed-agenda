protoc := protoc
python := python

.PHONY: pbc
pbc:go-protoc py-protoc;

.PHONY: go-protoc
go-protoc:
	$(protoc) --proto_path=./protos --go_out=. --go-grpc_out=. ./protos/*.proto

.PHONY: py-protoc
py-protoc:
	$(python) -m grpc_tools.protoc --proto_path=./protos --python_out=./client/proto --python_grpc_out=./client/proto \
	 --mypy_out=./client/proto ./protos/*.proto