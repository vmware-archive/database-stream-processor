#!/bin/bash

# Delete a project.

set -e

if [ "$#" -ne 1 ]; then
    echo "Usage '$0 <project_id>'"
    exit 1
fi

curl -X DELETE http://localhost:8080/projects/$1 -H 'Content-Type: application/json' -d '{}'
