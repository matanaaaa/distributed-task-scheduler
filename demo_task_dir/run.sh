#!/bin/sh
set -e
mkdir -p output
echo "start $(date)" > output/timeline.txt
sleep 20
echo "end $(date)" >> output/timeline.txt