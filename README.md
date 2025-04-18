# go-db-etl

go-db-etl is intended to be a handy tool for loading data out of mutliple data sources (possibly transform them) and insert into another data source. 
This may include targets such as Data Warehouses. 

The plan is to support multiple types of source systems such as: 
- files
- mysql
- postgresql
- MSSQL
- and possibly more


# Docker
In order to run the docker compose execute the following statement from the root of this project
```
docker compose -f .\build\docker-compose.yml up --build -d
```