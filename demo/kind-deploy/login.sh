#!/bin/sh

# Login to the demo pod terminal with an environment variable pointing to the Redpanda brokers
kubectl run -i --tty demo-pod --image=localhost:5001/dbspdemo --restart=Never --env="REDPANDA_BROKERS=`kubectl get po redpanda-0 --template '{{.status.podIP}}'`:9093" -- bash
