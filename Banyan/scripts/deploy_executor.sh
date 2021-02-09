#!/bin/bash

aws s3 cp scripts/executor.jl s3://banyan-executor/
aws s3 cp scripts/queues.jl s3://banyan-executor/