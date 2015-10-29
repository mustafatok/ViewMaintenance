#!/bin/bash
count=0
while IFS='' read -r line || [[ -n "$line" ]]; do
    echo "Text read from file: $line"
    if [ $count -ne 0 ] ; then
        scp ./ips.txt jan@$line:/home/jan/ViewMaintenance
    fi
    count+=1
    ssh jan@$line 'bash -s' < setupIps.sh "$1"
done < "$1"
