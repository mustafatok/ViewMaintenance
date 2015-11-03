#!/bin/bash
while IFS='' read -r line || [[ -n "$line" ]]; do
    echo "Text read from file: $line"
    ssh jan@$line 'bash -s' < clean.sh "$1"
done < "$1"
