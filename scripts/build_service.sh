#!/bin/zsh
set -euo pipefail

echo "BUILDING IMAGE"
docker-compose build

echo "SPINNING UP CONTAINER"
docker-compose up -d

echo "GET DOCKER ID"
CONTAINER_ID="$(docker ps -aqf 'name=^disease-mapping')"
echo $CONTAINER_ID

echo "LOAD GRAPH"
docker exec $CONTAINER_ID bin/neo4j-admin import --database=dbdiseasemapping --delimiter TAB --high-io=true --skip-duplicate-nodes=true --multiline-fields=true --force \
  --nodes='import/nodes.csv' \
  --relationships='import/relationships.csv'

echo "RESTARTING CONTAINER WITH NEW GRAPH"
docker-compose restart

sleep 60

echo "SET INDEX"
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :DOID(source_id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :EFO(source_id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :HPO(source_id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :ICD9CM(source_id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :ICD10CM(source_id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :ICD11(source_id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :ICDO-3(source_id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :KEGG(source_id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :MEDDRA(source_id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :MEDGEN(source_id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :MESH(source_id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :MONDO(source_id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :NCIT(source_id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :ORPHANET(source_id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :SNOMEDCT(source_id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :UMLS(source_id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :DOID(id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :EFO(id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :HPO(id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :ICD9CM(id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :ICD10CM(id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :ICD11(id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :ICDO-3(id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :KEGG(id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :MEDDRA(id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :MEDGEN(id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :MESH(id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :MONDO(id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :NCIT(id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :ORPHANET(id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :SNOMEDCT(id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :UMLS(id)'
docker exec $CONTAINER_ID bin/cypher-shell -u $NEO4J_USER -p $NEO4J_PASS 'create index on :SYNONYM(name)'
