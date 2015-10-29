#!/bin/bash
cd /home/jan/ViewMaintenance
count=0
hostnamePrefix="vm-"
hostname=".cloud.mwn.de"
while IFS='' read -r line || [[ -n "$line" ]]; do
    printf "handling $count line\n"
    if [ $count -eq 0 ] ; then
        firstIp=$line
        firstHostName=$hostnamePrefix${line//./-}$hostname
        else
            stringHostName+=","
            stringRegionServers+=$"\n"
    fi
    count+=1
    # generate host names according to ip: vm-AAA-BBB-CCC-DDD.cloud.mwn.de
    stringHostName+=$hostnamePrefix${line//./-}$hostname
    stringRegionServers+=$hostnamePrefix${line//./-}$hostname
done < "$1"

printf "generated host name string :\n$stringHostName\n"
printf "generated region servers: \n$stringRegionServers\n"

# save region servers to hbase conf file
echo -e "$stringRegionServers" > hbase-0.98.12-hadoop1/conf/regionservers

# generate hbase-site.xml
hbsb=$(<hbase-site-begin.txt)
hbsm=$(<hbase-site-middle.txt)
hbse=$(<hbase-site-end.txt)
hbs=$hbsb$firstHostName$hbsm$stringHostName$hbse
printf "generated hvase-site.xml file: \n$hbs\n"
echo "$hbs" > hbase-0.98.12-hadoop1/conf/hbase-site.xml 

# generate core-site.xml of hadoop
hcsb=$(<core-site-begin.txt)
hcse=$(<core-site-end.txt)
hcs=$hcsb$firstIp$hcse
printf "generated hadoop core-site.xml file: \n $hcs\n"
echo "$hcs" > hadoop-1.2.1/conf/core-site.xml

# use ip list to generate slaves file in hadoop
cat "$1" > hadoop-1.2.1/conf/slaves
# use the first ip to set the master file in hadoop
echo "$firstIp" > hadoop-1.2.1/conf/masters
