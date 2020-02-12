#! /bin/bash

postgresql_9_3() {
    docker rm -f postgres || true
    docker run --name postgres -e POSTGRES_DB=test -e POSTGRES_PASSWORD=postgres -p5432:5432 -d postgres:9.3
}

if [ -z ${1} ]; then
    echo "No db name provided"
    echo "Provide one of:"
    echo -e "\postgresql_9_3"
else
    ${1}
fi