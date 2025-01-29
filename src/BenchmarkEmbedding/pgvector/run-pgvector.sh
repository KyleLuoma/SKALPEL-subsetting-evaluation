docker run -it --name skalpel-pgvector -p 5432:5432 \
    -e POSTGRES_PASSWORD=skalpel \
    -v ./pgdata:/var/lib/postgresql/data \
    skalpel-pgvector