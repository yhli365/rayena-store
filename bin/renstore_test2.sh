#!/bin/bash
# nohup sh renstore_test2.sh >> renstore_test2.out 2>&1 &
# yanhong@bjrun.com 20150112
# 
# nodes:1, nrFiles=2,fileSize=120
# nodes:10, nrFiles=40,fileSize=2500000

nrFiles=2
fileSize=120
sleepSeconds=5

echo "**********************************************************************"
echo "$(date '+%Y/%m/%d %H:%M:%S') - Begin to execute test ..."
echo "$(date '+%Y/%m/%d %H:%M:%S') - <conf> nrFiles=$nrFiles, fileSize=$fileSize, sleepSeconds=$sleepSeconds"
storedate="19990101"
cat renstore_test2.cfg | while read line
do
	mytime=$line
	echo -e "\n$(date '+%Y/%m/%d %H:%M:%S') - exec date: $mytime"
	if [ $mytime ]; then
		echo -e "\n$(date '+%Y/%m/%d %H:%M:%S') - exec mr[object.gen] ---------------"
		hadoop jar ayena-store-1.0.jar object.gen -Dmapreduce.job.name='odg'$mytime -Dtype=ren -DnrFiles=$nrFiles -DfileSize=$fileSize -Ddate=$mytime /testdata/ren/$mytime
		
		echo -e "\n$(date '+%Y/%m/%d %H:%M:%S') - exec hdfs[du] ---------------"
		hdfs dfs -du -s /testdata/ren/$mytime
		hdfs dfs -du -s /testdata/ren/$mytime/*
		
		echo "$(date '+%Y/%m/%d %H:%M:%S') - Sleep '$sleepSeconds's."
		sleep $sleepSeconds
		
		echo -e "\n$(date '+%Y/%m/%d %H:%M:%S') - exec mr[object.merge] ---------------"
		hadoop jar ayena-store-1.0.jar object.merge2 -Dmapreduce.job.name='om'$mytime -Dtype=ren -Dobjectstore.dir=/testdata/renstore/$storedate/part-r-* /testdata/ren/$mytime /testdata/renstore/$mytime
		storedate=$mytime
		
		echo -e "\n$(date '+%Y/%m/%d %H:%M:%S') - exec hdfs[du] ---------------"
		hdfs dfs -du -s /hbase/data/default/ren*
		
		echo "$(date '+%Y/%m/%d %H:%M:%S') - Sleep '$sleepSeconds's."
		sleep $sleepSeconds
	fi
done
echo "$(date '+%Y/%m/%d %H:%M:%S') - End to execute test!!!"
