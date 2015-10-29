#!/bin/bash
./batchSetupIps.sh ips.txt
hadoop-1.2.1/bin/hadoop namenode -format
hadoop-1.2.1/bin/start-dfs.sh

