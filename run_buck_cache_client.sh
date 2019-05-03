#!/usr/bin/env bash

./gradlew distJar

$JAVA_HOME/bin/java -Xmx4G  -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:MaxDirectMemorySize=6g -Dlog.home=/var/log/buck-cache-client/logs -jar cache/build/libs/cache-1.0.4-standalone.jar $1.yml
