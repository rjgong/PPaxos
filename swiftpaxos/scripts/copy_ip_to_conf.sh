#!/bin/bash

base_path=${1:-'swiftpaxos_copy/swiftpaxos'}
json_file="$base_path/${2:-scripts/conf.json}"
conf_file="$base_path/${3:-local.conf}"
output_file="$base_path/${4:-out.conf}"

# output_file should not exist yet
rm -f $output_file

PASSSED_PROXY=false
while IFS= read -r line; do
    if [ "$line" == "-- Proxy --" ]; then
        PASSSED_PROXY=true
    fi

    if [ "$PASSED_PROXY" = "true" ]; then
        echo "$line" >> $output_file
    else
        ARR=($line)
        ADDRESS=$(jq -r --arg ALIAS_NAME "${ARR[0]}" '.nodes[] | select(.alias == $ALIAS_NAME).node_address' $json_file)
        if [ ! -z "$ADDRESS" ]; then
            echo "${ARR[0]} $ADDRESS" >> $output_file
        else
            echo "$line" >> $output_file
        fi
    fi
done < $conf_file
