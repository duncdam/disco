FROM neo4j:4.4.12-community

COPY neo4j/conf /conf

COPY neo4j/dbs /data

COPY neo4j/logs /logs

COPY neo4j/import /var/lib/neo4j/import

COPY neo4j/plugins /var/lib/neo4j/plugins

EXPOSE 7474

EXPOSE 7687