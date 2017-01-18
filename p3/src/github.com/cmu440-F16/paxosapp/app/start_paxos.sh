#!/bin/bash

if [ -z $GOPATH ]; then
    echo "FAIL: GOPATH environment variable is not set"
    exit 1
fi


# Build the student's paxos node implementation.
# Exit immediately if there was a compile-time error.
go install github.com/cmu440-F16/paxosapp/runners/prunner
if [ $? -ne 0 ]; then
   echo "FAIL: code does not compile"
   exit $?
fi


# Pick random ports between [10000, 20000).
NODE_PORT0=$(((RANDOM % 10000) + 10000))
NODE_PORT1=$(((RANDOM % 10000) + 10000))
NODE_PORT2=$(((RANDOM % 10000) + 10000))
NODE_PORT3=$(((RANDOM % 10000) + 10000))
NODE_PORT4=$(((RANDOM % 10000) + 10000))
NODE_PORT5=$(((RANDOM % 10000) + 10000))
NODE_PORT6=$(((RANDOM % 10000) + 10000))
NODE_PORT7=$(((RANDOM % 10000) + 10000))
NODE_PORT8=$(((RANDOM % 10000) + 10000))
NODE_PORT9=$(((RANDOM % 10000) + 10000))

PAXOS_NODE=$GOPATH/bin/prunner
ALL_PORTS="${NODE_PORT0},${NODE_PORT1},${NODE_PORT2},${NODE_PORT3},\
${NODE_PORT4},${NODE_PORT5},${NODE_PORT6},${NODE_PORT7},\
${NODE_PORT8},${NODE_PORT9}"
echo ${ALL_PORTS}
##################################################

# Start paxos nodes.
${PAXOS_NODE} -myport=${NODE_PORT0} -ports=${ALL_PORTS} -N=10 -id=0 -retries=11 &
PAXOS_NODE_PID0=$!
sleep 1

${PAXOS_NODE} -myport=${NODE_PORT1} -ports=${ALL_PORTS} -N=10 -id=1 -retries=11 &
PAXOS_NODE_PID1=$!
sleep 1

${PAXOS_NODE} -myport=${NODE_PORT2} -ports=${ALL_PORTS} -N=10 -id=2 -retries=11 &
PAXOS_NODE_PID2=$!
sleep 1

${PAXOS_NODE} -myport=${NODE_PORT3} -ports=${ALL_PORTS} -N=10 -id=3 -retries=11 &
PAXOS_NODE_PID3=$!
sleep 1

${PAXOS_NODE} -myport=${NODE_PORT4} -ports=${ALL_PORTS} -N=10 -id=4 -retries=11 &
PAXOS_NODE_PID4=$!
sleep 1

${PAXOS_NODE} -myport=${NODE_PORT5} -ports=${ALL_PORTS} -N=10 -id=5 -retries=11 &
PAXOS_NODE_PID5=$!
sleep 1

${PAXOS_NODE} -myport=${NODE_PORT6} -ports=${ALL_PORTS} -N=10 -id=6 -retries=11 &
PAXOS_NODE_PID6=$!
sleep 1

${PAXOS_NODE} -myport=${NODE_PORT7} -ports=${ALL_PORTS} -N=10 -id=7 -retries=11 &
PAXOS_NODE_PID7=$!
sleep 1

${PAXOS_NODE} -myport=${NODE_PORT8} -ports=${ALL_PORTS} -N=10 -id=8 -retries=11 &
PAXOS_NODE_PID8=$!
sleep 1

${PAXOS_NODE} -myport=${NODE_PORT9} -ports=${ALL_PORTS} -N=10 -id=9 -retries=11 &
PAXOS_NODE_PID9=$!
sleep 2

echo "All paxos started; now start the webapp"
