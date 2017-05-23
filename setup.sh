#!/bin/bash

### Shell script to spin up a docker container for postgresql.

## color codes
RED='\033[1;31m'
GREEN='\033[1;32m'
YELLOW='\033[1;33m'
CYAN='\033[1;36m'
PLAIN='\033[0m'

## variables
INFORMIX_CONTAINER="informix_c"
INFORMIX_IMAGE="informix-db"
LOGFILE="logfile"

HOST="localhost"
USER="informix"
PASSWORD="in4mix"
PORT1=9088
PORT2=9089
DATABASE="loopback"

## check if docker exists
printf "\n${RED}>> Checking for docker${PLAIN} ${GREEN}...${PLAIN}"
docker -v > /dev/null 2>&1
DOCKER_EXISTS=$?
if [ "$DOCKER_EXISTS" -ne 0 ]; then
    printf "\n\n${CYAN}Status: ${PLAIN}${RED}Docker not found. Terminating setup.${PLAIN}\n\n"
    exit 1
fi
printf "\n${CYAN}Found docker. Moving on with the setup.${PLAIN}\n"

## cleaning up previous builds
printf "\n${RED}>> Finding old builds and cleaning up${PLAIN} ${GREEN}...${PLAIN}"
docker rm -f $INFORMIX_CONTAINER > /dev/null 2>&1
printf "\n${CYAN}Clean up complete.${PLAIN}\n"

## build database container
printf "\n${RED}>> Building database container${PLAIN} ${GREEN}...${PLAIN}"
docker build -t $INFORMIX_IMAGE /database/ > /dev/null 2>&1
printf "\n${CYAN}Database container built.${PLAIN}\n"

# put up the database service
printf "\n${RED}>> Starting the database service${PLAIN} ${GREEN}...${PLAIN}\n"
docker run -itd --name $INFORMIX_CONTAINER --privileged -p $PORT1:9088 -p $PORT2:9089 $INFORMIX_IMAGE:latest > /dev/null 2>&1

## timeout here to start the database and setup schema inside informix
TIMEOUT=120
TIME_PASSED=0
WAIT_STRING="."
STARTUP_MESSAGE="Startup of dev SUCCESS"

docker logs $INFORMIX_CONTAINER > $LOGFILE
grep -q "$INFORMIX_CONTAINER" $LOGFILE
OUTPUT=$?

printf "\n${GREEN}Waiting for database server to start $WAIT_STRING${PLAIN}"
while [ "$OUTPUT" -ne 0 ] && [ "$TIMEOUT" -gt 0 ]
    do
        docker logs $INFORMIX_CONTAINER > logfile
        grep -q "$STARTUP_MESSAGE" logfile
        OUTPUT=$?
        sleep 1s
        TIMEOUT=$((TIMEOUT - 1))
        TIME_PASSED=$((TIME_PASSED + 1))

        if [ "$TIME_PASSED" -eq 5 ]; then
            printf "${GREEN}.${PLAIN}"
            TIME_PASSED=0
        fi
    done
if [ "$TIMEOUT" -le 0 ]; then
    printf "\n\n${CYAN}Status: ${PLAIN}${RED}Failed to start the database. Terminating setup.${PLAIN}\n\n"
    exit 1
fi
printf "\n${CYAN}Container is up and running.${PLAIN}\n"

## set env variables for running test
printf "\n${RED}>> Setting env variables to run test${PLAIN} ${GREEN}...${PLAIN}"
export INFORMIX_HOSTNAME=$HOST
export INFORMIX_PORTNUM=$PORT
export INFORMIX_USERNAME=$USER
export INFORMIX_PASSWORD=$PASSWORD
export INFORMIX_DATABASE=$DATABASE
export CI=true
rm $LOGFILE
printf "\n${CYAN}Env variables set.${PLAIN}\n"

printf "\n${CYAN}Status: ${PLAIN}${GREEN}Set up completed successfully.${PLAIN}\n"
printf "\n${CYAN}To run the test suite:${PLAIN} ${YELLOW}npm test${PLAIN}\n\n"
