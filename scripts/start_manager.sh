#!/bin/bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT_DIR="${THIS_DIR}/.."
SQL_COMPILER_DIR="${ROOT_DIR}/sql-to-dbsp-compiler"
MANAGER_DIR="${ROOT_DIR}/pipeline_manager"

if [ "$#" -lt 1 ]; then
    echo "Usage '$0 <working_directory_path> <bind address (optional)>'"
    exit 1
fi

# This is the most portable way to get an absolute path since
# 'realpath' is not available on MacOS by default.
WORKING_DIR=$(cd "$(dirname "${1}")" && pwd -P)/$(basename "${1}")
DEFAULT_BIND_ADDRESS="127.0.0.1"
BIND_ADDRESS="${2:-$DEFAULT_BIND_ADDRESS}"

# Kill manager. pkill doesn't handle process names >15 characters.
pkill -9 dbsp_pipeline_

set -e

cd "${MANAGER_DIR}" && ~/.cargo/bin/cargo build --release
cd "${MANAGER_DIR}" && ~/.cargo/bin/cargo run --release -- \
    --bind-address="${BIND_ADDRESS}" \
    --working-directory="${WORKING_DIR}" \
    --sql-compiler-home="${SQL_COMPILER_DIR}" \
    --dbsp-override-path="${ROOT_DIR}" \
    --static-html=static \
    --unix-daemon
