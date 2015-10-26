#!/bin/bash
cd /home/jan/ViewMaintenance
count=0
while IFS='' read -r line || [[ -n "$line" ]]; do
    $count = $count + 1
    printf "handling $count line\n"
    if [ $count -eq 0 ] ; then
        firstIp=$line
        else
            stringHostName+=","
    fi
    # generate host names according to ip: vm-AAA-BBB-CCC-DDD.cloud.mwn.de
    hostname=".cloud.mwn.de"
    stringHostName+=${line//./-}$hostname
done < "$1"

printf "generated host name string :\n$stringHostName\n"

# generate core-site.xml of hadoop
hcsb=$(<core-site-begin.txt)
hcse=$(<core-site-end.txt)
hcs=$hcsb$firstIp$hcse
printf "generated hadoop core-site.xml file: \n $hcs"
echo "$hcs" > hadoop-1.2.1/conf/core-site.xml

# use ip list to generate slaves file in hadoop
cat "$1" > hadoop-1.2.1/conf/slaves
# use the first ip to set the master file in hadoop
echo "$firstIp" > hadoop-1.2.1/conf/masters
