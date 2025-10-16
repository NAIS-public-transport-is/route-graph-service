#!/usr/bin/env bash
set -euo pipefail

NEO4J_PASS=${1:-test1234}

CONTAINER_NAME=neo4j-local
IMAGE=neo4j:5

echo "Starting Neo4j..."
docker run -d --rm --name $CONTAINER_NAME -p7474:7474 -p7687:7687 \
    -e NEO4J_AUTH=neo4j/$NEO4J_PASS \
    $IMAGE

echo "Waiting for Neo4j to accept connections..."
# wait for bolt port to be open
for i in $(seq 1 60); do
  if docker exec $CONTAINER_NAME bash -c "echo 'RETURN 1' | cypher-shell -u neo4j -p $NEO4J_PASS >/dev/null 2>&1"; then
    echo "Neo4j ready."
    break
  fi
  echo -n "."
  sleep 1
done

echo "Pushing seed..."
SEED_FILE="seed/seed.cypher"

cat $SEED_FILE | docker exec -i $CONTAINER_NAME cypher-shell -u neo4j -p $NEO4J_PASS

echo "Seed completed."
