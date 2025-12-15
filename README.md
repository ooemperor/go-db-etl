# go-db-etl
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![GoDoc](https://godoc.org/github.com/ooemperor/go-etl?status.svg)](https://pkg.go.dev/github.com/ooemperor/go-db-etl@v1.0.0)
[![Go Report Card](https://goreportcard.com/badge/github.com/ooemperor/go-db-etl)](https://goreportcard.com/report/github.com/ooemperor/go-db-etl)
[![GitHub release](https://img.shields.io/github/tag/ooemperor/go-db-etl.svg?label=release)](https://github.com/ooemperor/go-db-etl/releases)
[![GitHub release date](https://img.shields.io/github/release-date/ooemperor/go-db-etl.svg)](https://github.com/ooemperor/go-db-etl/releases)


## Actions
[![CI](https://github.com/ooemperor/go-db-etl/actions/workflows/docker_build.yml/badge.svg)](https://github.com/ooemperor/go-db-etl/actions/workflows/docker_build.yml)
[![CI](https://github.com/ooemperor/go-db-etl/actions/workflows/go_test.yml/badge.svg)](https://github.com/ooemperor/go-db-etl/actions/workflows/go_test.yml)
[![CI](https://github.com/ooemperor/go-db-etl/actions/workflows/docker_build_publish_main.yml/badge.svg)](https://github.com/ooemperor/go-db-etl/actions/workflows/docker_build_publish_main.yml)
[![CI](https://github.com/ooemperor/go-db-etl/actions/workflows/docker_build_publish_development.yml/badge.svg)](https://github.com/ooemperor/go-db-etl/actions/workflows/docker_build_publish_development.yml)



Private repo for CI/CD orchestrations.

## Description

go-db-etl is intended to be a handy tool for loading data out of mutliple data sources and insert into another data source. This may include targets such as Data Warehouses.  
Further more it will build a history of the data loads in a so called raw data vault. (rdv). 
The load history has been inspired by 


The plan is to support multiple types of source systems such as: 
- files
  - json
  - csv
- mysql
  - mariadb
  - innodb
- postgresql
- MSSQL
- neo4j
  - via JSON export


## Docker
### Images
Docker images are pubished on the github container registry:
- Main branch: `ghcr.io/ooemperor/go-db-etl:main`
- Development branch: `ghcr.io/ooemperor/go-db-etl:development`

### Compose
If you want to build and run via docker compose you can use the following command:
```sh
docker compose -f .\build\docker-compose.yml up --build -d
```