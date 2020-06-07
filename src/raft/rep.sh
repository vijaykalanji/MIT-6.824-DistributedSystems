#!/bin/bash

TEST=$1
TMP=./repeat.tmp
GTIMEOUT=gtimeout
WAIT=$2

for i in `seq 1 $3`;
do 
	echo "Trial #$i: $TEST"
	$GTIMEOUT $WAIT go test -run $TEST > $TMP 2>&1
	if [[ $? != 0 ]]; then
		tail -n1 $TMP | grep -i "FAIL"  > /dev/null 2> /dev/null
		if [[ $? == 0 ]]; then
			echo "FAIL: test failed"
		else
			echo "FAIL: test timed out!" 
		fi
		exit -1
	fi
	tail -n1 $TMP | egrep -i "(pass|ok)" > /dev/null 2> /dev/null
	if [[ $? != 0 ]]; then
		STATUS=`tail -n1 $TMP | awk '{print $1}'`
		echo "FAIL: $STATUS"
		exit -1
    else 
        echo "...passed"
    fi
done

echo "PASS: all ($1) tests completed without timeout!"
