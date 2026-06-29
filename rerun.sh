#!/bin/bash
set -e
set -x

mkdir -p ~/recordings
mvn install -DskipTests -Dktlint.skip
docker build -t jitsi-multitrack-recorder:latest .
docker run -d \
  --name jitsi-multitrack-recorder \
  --restart unless-stopped \
  -e JMR_FINALIZE_SCRIPT=/scripts/finalize-flatten.sh \
  -p 8989:8989 \
  -v /root/recordings:/data \
  jitsi-multitrack-recorder:latest
