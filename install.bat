@echo off
echo "install mangocity-data"
start "mangocity-data install" mvn clean install -Dmaven.test.skip
pause