neo4j-admin import --database=db20221010 --delimiter TAB --high-io=true --skip-duplicate-nodes=true \
--nodes='import/nodes.csv' \
--relationships='import/relationships.csv'