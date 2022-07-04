protoc := protoc
python := python
go := go
make := make

.PHONY: client
client:
	$(make) -C ./client client python=$(python)

.PHONY: client-install
client-install:
	$(make) -C ./client install python=$(python)

.PHONY: server
server:
	$(make) -C ./server server go=$(go)

.PHONY: server-install
server-install:
	$(make) -C ./server install go=$(go)

.PHONY: pbc
pbc:go-protoc py-protoc chord-protoc;

.PHONY: go-protoc
go-protoc:
	$(protoc) --proto_path=. --go_out=. --go-grpc_out=. ./proto/*.proto

.PHONY: py-protoc
py-protoc:
	$(python) -m grpc_tools.protoc --proto_path=. --python_out=./client --python_grpc_out=./client \
	 --mypy_out=./client -I./client/ ./proto/*.proto

.PHONY: chord-protoc
chord-protoc:
	$(make) -C ./server/chord chord-protoc go=$(go)