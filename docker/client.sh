#!/usr/bin/env bash

configFile=$1	
nTests=$2
logConfig=$3
resultsPath=$4
nValues=$5

# /home/client needs to be in first in the classpath 
# or the configuration file is ignored

java -cp $CLASSPATH:/usr/local/hbase/hbase-0.98.22-hadoop2/lib/*:hbase-client.xml\
 -Dlog4j.configuration=file:$logConfig\
  pt.uminho.haslab.safecloudclient.shareclient.benchmarks.Main $configFile $nTests $resultsPath $nValues
