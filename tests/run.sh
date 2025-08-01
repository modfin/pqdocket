#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")"

echo "Starting postgres instance if its not up already"
[[ $(docker ps -f "name=pqdocket-test-db" --format '{{.Names}}') == 'pqdocket-test-db' ]] || \
  docker run --rm -d -p 9300:5432 --name pqdocket-test-db -e POSTGRES_HOST_AUTH_METHOD=trust --tmpfs /var/lib/postgresql/data:rw postgres:17

function cleanup() {
  docker kill pqdocket-test-db > /dev/null
}
trap cleanup EXIT

echo "Waiting for database"

until printf 'CREATE EXTENSION IF NOT EXISTS "pgcrypto"' | docker exec -i pqdocket-test-db bash -c 'psql -U postgres' > /dev/null 2>&1; do
    sleep 1
done
echo "db is up"

printf 'TRUNCATE pqdocket_task_test' | docker exec -i pqdocket-test-db bash -c 'psql -U postgres' > /dev/null 2>&1

echo "Running tests..."
if [ -z $@ ]; then
  GORACE=halt_on_error=1 go test -timeout 99999s -race -count 1 ./
else
  GORACE=halt_on_error=1 go test -timeout 99999s -race -count 1 ./ -run $@
fi
