@echo off
echo "install mango_etl"
start "mango_etl install" mvn clean install -Dmaven.test.skip
pause