go mod tidy
go mod download
go build -o ../bin/go-db-etl ../cmd/main.go
