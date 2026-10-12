# Postgres with the schema and seed data baked in. (Baking it in avoids Docker
# Desktop file-sharing problems with bind-mounting a file from another drive.)
FROM postgres:16-alpine
COPY init.sql /docker-entrypoint-initdb.d/init.sql
