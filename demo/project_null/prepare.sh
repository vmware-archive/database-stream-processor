#!/bin/bash

rpk topic delete null_demo_input
rpk topic delete null_demo_output

rpk topic create null_demo_input -c retention.ms=-1 -c retention.bytes=-1
rpk topic create null_demo_output
