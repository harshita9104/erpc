#!/bin/bash

set -x

export $(grep -v '^#' config/primary/.env | xargs -d '\n')
sudo docker exec neo4j_neo4j_1 cypher-shell -u $NEO4J_DB_USERNAME -p $NEO4J_DB_PASSWORD --file /queries/neo4j_gds_wcc_list.cql > neo4j_gds_wcc_list.txt
