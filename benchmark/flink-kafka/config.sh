#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

target="$0"
# For the case, the executable has been directly symlinked, figure out
# the correct bin path by following its symlink up to an upper bound.
# Note: we can't use the readlink utility here if we want to be POSIX
# compatible.
iteration=0
while [ -L "$target" ]; do
    if [ "$iteration" -gt 100 ]; then
        echo "Cannot resolve path: You have a cyclic symlink in $target."
        break
    fi
    ls=`ls -ld -- "$target"`
    target=`expr "$ls" : '.* -> \(.*\)$'`
    iteration=$((iteration + 1))
done

FLINK_HOME=/opt/flink
export FLINK_HOME

# Convert relative path to absolute path and resolve directory symlinks
NEXMARK_HOME=/opt/nexmark
NEXMARK_LIB_DIR=$NEXMARK_HOME/lib
NEXMARK_QUERY_DIR=$NEXMARK_HOME/queries
NEXMARK_LOG_DIR=$NEXMARK_HOME/log
NEXMARK_CONF_DIR=$NEXMARK_HOME/conf
NEXMARK_BIN_DIR=$NEXMARK_HOME/bin

### Exported environment variables ###
export NEXMARK_HOME
export NEXMARK_LIB_DIR
export NEXMARK_QUERY_DIR
export NEXMARK_LOG_DIR
export NEXMARK_CONF_DIR
export NEXMARK_BIN_DIR

# Auxilliary function which extracts the name of host from a line which
# also potentially includes topology information and the taskManager type
extractHostName() {
    # handle comments: extract first part of string (before first # character)
    WORKER=`echo $1 | cut -d'#' -f 1`

    # Extract the hostname from the network hierarchy
    if [[ "$WORKER" =~ ^.*/([0-9a-zA-Z.-]+)$ ]]; then
            WORKER=${BASH_REMATCH[1]}
    fi

    echo $WORKER
}

readWorkers() {
    WORKERS_FILE="${FLINK_HOME}/conf/workers"

    if [[ ! -f "$WORKERS_FILE" ]]; then
        echo "No workers file. Please specify workers in 'conf/workers'."
        exit 1
    fi

    WORKERS=()

    WORKERS_ALL_LOCALHOST=true
    GOON=true
    while $GOON; do
        read line || GOON=false
        HOST=$( extractHostName $line)
        if [ -n "$HOST" ] ; then
            WORKERS+=(${HOST})
            if [ "${HOST}" != "localhost" ] && [ "${HOST}" != "127.0.0.1" ] ; then
                WORKERS_ALL_LOCALHOST=false
            fi
        fi
    done < <(sort -u "$WORKERS_FILE")
}

# starts or stops TMs on all workers
# TMWorkers start|stop
TMWorkers() {
    CMD=$1

    readWorkers

    if [ ${WORKERS_ALL_LOCALHOST} = true ] ; then
        # all-local setup
        for worker in ${WORKERS[@]}; do
          if [ "${CMD}" != "stop" ] ; then
            "${NEXMARK_BIN_DIR}"/side_input_gen.sh
          fi
          "${NEXMARK_BIN_DIR}"/metric_client.sh "${CMD}"
        done
    else
        # non-local setup
        # start/stop TaskManager instance(s)
        for worker in ${WORKERS[@]}; do
          if [[ $CMD == "start" ]] ; then
            ssh -n $worker -- "nohup /bin/bash -l $NEXMARK_BIN_DIR/side_input_gen.sh &"
            echo "Generated side input data on $worker"
            ssh -n $worker -- "nohup /bin/bash -l $NEXMARK_BIN_DIR/metric_client.sh start &>/dev/null &"
            echo "Started metric monitor on $worker"
          else
            ssh -n $worker -- "nohup /bin/bash -l $NEXMARK_BIN_DIR/metric_client.sh stop &"
            ssh -n $worker -- "nohup rm -rf $NEXMARK_HOME/data/output &"
          fi
        done
    fi
}
