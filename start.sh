#!/bin/sh
gradle wrapper --gradle-version 9.2.1
./gradlew build
./gradlew run
