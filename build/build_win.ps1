go mod tidy
go mod download
go build -o ../bin/go-db-etl.exe ../cmd/main.go
