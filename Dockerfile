FROM neo4j:4.4.12-community

#copy neo4j configure file
COPY neo4j/conf /conf

#copy databases
COPY neo4j/dbs /data

#copy logs folder
COPY neo4j/logs /logs

#copy import folder
COPY neo4j/import /var/lib/neo4j/import

#copy neccessary plugin files
COPY neo4j/plugins /var/lib/neo4j/plugins

#port for http and bolt
EXPOSE 7474
EXPOSE 7687