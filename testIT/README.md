# DG-Indexer integration Test

In this project, integration between application interfaces and external technologies will be tested. Such external technologies are next:

- Stratio Search Engine
- Postgres SQL Data Base

## How to start external technologies


```
docker run -dit --name postgres1 -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=governance -p 5432:5432 postgres
```


## How to run a postgres SQL shell

```
docker exec -it postgres1 psql -d governance -U postgres -W
```

```
docker stop postgres1
docker start postgres1
```

## How to run a Search Engine

```
git clone https://github.com/Stratio/search-engine-core.git
cd search-engine-core/local-env
docker-compose -f docker-compose-all.yml up 
```
