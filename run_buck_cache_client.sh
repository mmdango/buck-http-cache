#!/usr/bin/env bash

set -ex

./gradlew distJar

$JAVA_HOME/bin/java -Xmx16G  -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:MaxDirectMemorySize=6g -Dlog.home=/var/log/buck-cache-client/logs -jar cache/build/libs/cache-1.0.4-standalone.jar server cache/src/dist/config/$1.yml
