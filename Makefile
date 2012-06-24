-PHONY: all

all: client/peerdrive/peerdrive_client_pb2.py .deps
	cd server && rebar compile

client/peerdrive/peerdrive_client_pb2.py:
	protoc -Iserver/apps/peerdrive/src/ --python_out=client/peerdrive/ server/apps/peerdrive/src/peerdrive_client.proto

.deps:
	cd server && rebar get-deps
	touch .deps
