#!/bin/bash

TC=/sbin/tc
IF=$1 # Network interface 

PAIRS=( $* ) 
PAIRS=( ${PAIRS[@]:1} ) # Should be ip1 delay1 ip2 delay2 ...

IP=()
DELAY=()
for (( i=0; i<${#PAIRS[@]}; i+=2 ))
do 
   IP+=(${PAIRS[i]})
   DELAY+=(${PAIRS[i + 1]})
done

U32="$TC filter add dev $IF protocol ip parent 1: prio 1 u32"
create () {
	echo "=== SHAPING INIT ==="

	$TC qdisc add dev $IF root handle 1: htb
	for (( i=0; i<${#IP[@]}; i+=1 ))
	do 
		ip=${IP[i]}
		delay=${DELAY[i]}
		ID=$(($i+1))
		NETEM_ID=$(($ID+1))

		$TC class add dev $IF parent 1: classid 1:$ID htb rate 1000gbps
		$TC qdisc add dev $IF parent 1:$ID handle $NETEM_ID: netem delay $delay

		$U32 match ip dst $ip/32 flowid 1:$ID # Exact ip only
	done

	echo "=== SHAPING DONE ==="
}

clean () {
	echo "=== CLEAN INIT ==="

	$TC qdisc del dev $IF root

	echo "=== CLEAN DONE ==="
}
clean
create
