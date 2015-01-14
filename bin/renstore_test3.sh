#!/bin/bash
# nohup sh renstore_test3.sh >> renstore_test3.out 2>&1 &
# yanhong@bjrun.com 20150113
# 
# object.gen odgFiles,odgFileSize,odgIdNum
# object.merge omReduces,omStoreDate
# [nodes:10] odgFiles=40,odgFileSize=2500000,odgIdNum=25000000,odgFilesInit=320,omReduces=40

#init params
odgFiles=1
odgFileSize=120
odgIdNum=500
odgFilesInit=2
omReduces=1
omStoreDate="20140101"
dataDir=testdata
sleepSeconds=5

echo "**********************************************************************"
echo "$(date '+%Y/%m/%d %H:%M:%S') - Begin to execute test ..."
echo "$(date '+%Y/%m/%d %H:%M:%S') - <conf> dataDir=$dataDir, sleepSeconds=$sleepSeconds"

odgFilesTmp=$odgFilesInit
cat renstore_test3.cfg | while read line
do
	mytime=$line
	echo -e "\n$(date '+%Y/%m/%d %H:%M:%S') - exec date: $mytime"
	if [ $mytime ]; then
		echo -e "\n$(date '+%Y/%m/%d %H:%M:%S') - exec mr[object.gen] ---------------"
		hadoop jar ayena-store-1.0.jar object.gen -Dmapreduce.job.name='odg'$mytime -Dtype=ren -DnrFiles=$odgFilesTmp -DfileSize=$odgFileSize -Ddate=$mytime -Did.num=$odgIdNum /$dataDir/ren/$mytime
		odgFilesTmp=$odgFiles
		
		echo -e "\n$(date '+%Y/%m/%d %H:%M:%S') - exec hdfs[du] ---------------"
		hdfs dfs -du -s /$dataDir/ren/$mytime
		hdfs dfs -du -s /$dataDir/ren/$mytime/*
		
		echo "$(date '+%Y/%m/%d %H:%M:%S') - Sleep '$sleepSeconds's."
		sleep $sleepSeconds
		
		echo -e "\n$(date '+%Y/%m/%d %H:%M:%S') - exec mr[object.mergehdfs] ---------------"
		hadoop jar ayena-store-1.0.jar object.mergehdfs -Dmapreduce.job.name='omdfs'$mytime -Dtype=ren -Dobjectstore.dir=/$dataDir/renstore/$omStoreDate/*-r-* -Dmapreduce.job.reduces=$omReduces /$dataDir/ren/$mytime /$dataDir/renstore/$mytime
		omStoreDate=$mytime
		
		echo -e "\n$(date '+%Y/%m/%d %H:%M:%S') - exec mr[object.imphbase] ---------------"
		hadoop jar ayena-store-1.0.jar object.imphbase -Dmapreduce.job.name='oimphbase'$mytime -Dtype=ren /$dataDir/renstore/$mytime/part-r-* /$dataDir/renimphbase/$mytime
		
		echo -e "\n$(date '+%Y/%m/%d %H:%M:%S') - exec hdfs[du] ---------------"
		hdfs dfs -du -s /hbase/data/default/ren*
		
		echo "$(date '+%Y/%m/%d %H:%M:%S') - Sleep '$sleepSeconds's."
		sleep $sleepSeconds
	fi
done
echo "$(date '+%Y/%m/%d %H:%M:%S') - End to execute test!!!"
