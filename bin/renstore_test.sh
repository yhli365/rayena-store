#!/bin/bash
# nohup sh renstore_test.sh >> renstore_test.out 2>&1 &
# yanhong@bjrun.com 20150102
# 
# nodes:1, nrFiles=2,fileSize=120
# nodes:10, nrFiles=40,fileSize=2500000

nrFiles=2
fileSize=120
sleepSeconds=2

echo "**********************************************************************"
echo "$(date '+%Y/%m/%d %H:%M:%S') - Begin to execute test ..."
echo "$(date '+%Y/%m/%d %H:%M:%S') - <conf> nrFiles=$nrFiles, fileSize=$fileSize, sleepSeconds=$sleepSeconds"
cat renstore_test.cfg | while read line
do
	mytime=$line
	echo -e "\n$(date '+%Y/%m/%d %H:%M:%S') - exec date: $mytime"
	if [ $mytime ]; then
		echo -e "\n$(date '+%Y/%m/%d %H:%M:%S') - exec mr[object.gen] ---------------"
		hadoop jar ayena-store-1.0.jar object.gen -Dmapreduce.job.name='odg'$mytime'_A' -Dtype=ren -DnrFiles=$nrFiles -DfileSize=$fileSize -Ddate=$mytime /testdata/ren/$mytime'_A'
		
		echo -e "\n$(date '+%Y/%m/%d %H:%M:%S') - exec hdfs[du] ---------------"
		hdfs dfs -du -s /testdata/ren/$mytime'_A'
		hdfs dfs -du -s /testdata/ren/$mytime'_A'/*
		
		echo "$(date '+%Y/%m/%d %H:%M:%S') - Sleep '$sleepSeconds's."
		sleep $sleepSeconds
		
		echo -e "\n$(date '+%Y/%m/%d %H:%M:%S') - exec mr[object.merge] ---------------"
		hadoop jar ayena-store-1.0.jar object.merge -Dmapreduce.job.name='om'$mytime'_A' -Dtype=ren /testdata/ren/$mytime'_A' /testdata/renout/$mytime'_A'
		
		echo -e "\n$(date '+%Y/%m/%d %H:%M:%S') - exec hdfs[du] ---------------"
		hdfs dfs -du -s /hbase/data/default/ren*
		
		echo "$(date '+%Y/%m/%d %H:%M:%S') - Sleep '$sleepSeconds's."
		sleep $sleepSeconds
		
		echo -e "\n$(date '+%Y/%m/%d %H:%M:%S') - exec mr[object.gen] ---------------"
		hadoop jar ayena-store-1.0.jar object.gen -Dmapreduce.job.name='odg'$mytime'_B' -Dtype=ren -DnrFiles=$nrFiles -DfileSize=$fileSize -Ddate=$mytime /testdata/ren/$mytime'_B'
		
		echo -e "\n$(date '+%Y/%m/%d %H:%M:%S') - exec hdfs[du] ---------------"
		hdfs dfs -du -s /testdata/ren/$mytime'_B'
		hdfs dfs -du -s /testdata/ren/$mytime'_B'/*
		
		echo "$(date '+%Y/%m/%d %H:%M:%S') - Sleep '$sleepSeconds's."
		sleep $sleepSeconds
		
		echo -e "\n$(date '+%Y/%m/%d %H:%M:%S') - exec mr[object.merge] ---------------"
		hadoop jar ayena-store-1.0.jar object.merge -Dmapreduce.job.name='om'$mytime'_B' -Dtype=ren /testdata/ren/$mytime'_B' /testdata/renout/$mytime'_B'
		
		echo -e "\n$(date '+%Y/%m/%d %H:%M:%S') - exec hdfs[du] ---------------"
		hdfs dfs -du -s /hbase/data/default/ren*
		
		echo "$(date '+%Y/%m/%d %H:%M:%S') - Sleep '$sleepSeconds's."
		sleep $sleepSeconds
	fi
done
echo "$(date '+%Y/%m/%d %H:%M:%S') - End to execute test!!!"
