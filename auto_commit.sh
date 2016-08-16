#!/bin/bash
count=0
while [ $count -lt 1000000 ]
do
    echo $count
     count=$(($count+1))
    git add 
    git commit -m "auto_commit_2016_08_16 16:33"
    sleep 500
done
