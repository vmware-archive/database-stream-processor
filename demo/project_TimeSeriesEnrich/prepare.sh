#!/bin/bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

rpk topic delete fraud_demo_large_demographics
rpk topic delete fraud_demo_large_transactions
rpk topic delete fraud_demo_large_enriched

rpk topic create fraud_demo_large_demographics -c retention.ms=-1 -c retention.bytes=-1
rpk topic create fraud_demo_large_transactions -c retention.ms=-1 -c retention.bytes=-1
rpk topic create fraud_demo_large_enriched

# Push test data to topics.

while mapfile -t -n 10000 ary && ((${#ary[@]})); do
    printf '%s\n' "${ary[@]}" | rpk topic produce fraud_demo_large_demographics -f '%v'
done < "${THIS_DIR}"/demographics.csv

while mapfile -t -n 10000 ary && ((${#ary[@]})); do
    printf '%s\n' "${ary[@]}" | rpk topic produce fraud_demo_large_transactions -f '%v'
done < "${THIS_DIR}"/transactions.csv
