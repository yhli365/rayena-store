#!/bin/bash
# nohup sh renstore_compress.sh >> renstore_compress.out 2>&1 &
# yanhong@bjrun.com 20150126
# 
# object.gen odgFiles,odgFileSize,odgIdNum
# [nodes:10] odgFiles=10,odgFileSize=5000000,odgIdNum=25000000

#init params
compressarr=("snappy" "lzo" "gzip")
times=1
odgFiles=1
odgFileSize=120
odgIdNum=500
dataDir=testdata
sleepSeconds=5

echo "**********************************************************************"
echo "$(date '+%Y/%m/%d %H:%M:%S') - Begin to execute test ..."
echo "$(date '+%Y/%m/%d %H:%M:%S') - <conf> dataDir=$dataDir, sleepSeconds=$sleepSeconds"

#none
for (( i=1; i<=$times; i++)); do
	echo -e "\n$(date '+%Y/%m/%d %H:%M:%S') - exec mr[object.gen] ---------------"
	hadoop jar ayena-store-1.0.jar object.gen -Dmapreduce.job.name='odg-none_'$i -Dtype=ren -DnrFiles=$odgFiles -DfileSize=$odgFileSize -Ddate=20150126 -Did.num=$odgIdNum -Dcompression.type=none /$dataDir/compress/'none_'$i

	echo -e "\n$(date '+%Y/%m/%d %H:%M:%S') - exec hdfs[du] ---------------"
	hdfs dfs -du -s /$dataDir/compress/'none_'$i

	echo "$(date '+%Y/%m/%d %H:%M:%S') - Sleep '$sleepSeconds's."
	sleep $sleepSeconds
done

for compress in ${compressarr[@]}
do
	for (( i=1; i<=$times; i++)); do  
		echo -e "\n$(date '+%Y/%m/%d %H:%M:%S') - exec mr[object.gen] ---------------"
		hadoop jar ayena-store-1.0.jar object.gen -Dmapreduce.job.name='odg-'$compress'_'$i -Dtype=ren -DnrFiles=$odgFiles -DfileSize=$odgFileSize -Ddate=20150126 -Did.num=$odgIdNum -Dcodec=$compress /$dataDir/compress/$compress'_'$i
		
		echo -e "\n$(date '+%Y/%m/%d %H:%M:%S') - exec hdfs[du] ---------------"
		hdfs dfs -du -s /$dataDir/compress/$compress'_'$i
		
		echo "$(date '+%Y/%m/%d %H:%M:%S') - Sleep '$sleepSeconds's."
		sleep $sleepSeconds
	done
done
echo "$(date '+%Y/%m/%d %H:%M:%S') - End to execute test!!!"
