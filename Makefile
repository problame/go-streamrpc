generate:
	protoc -I=. --go_out=internal/pdu main.proto
