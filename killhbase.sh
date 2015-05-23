#!/bin/bash
# file: killhbase.sh
jps
IN=$(jps)
read -a ary <<<$IN
for key in "${!ary[@]}"
do  
    if [ "${ary[$key]}" = "HMaster" -o "${ary[$key]}" = "HQuorumPeer" ]
    then
        echo "killing ${ary[$key-1]}"
        kill -9 "${ary[$key-1]}"
    fi
done
