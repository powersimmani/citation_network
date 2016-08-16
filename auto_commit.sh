#!/bin/bash
count=0
while [ $count -lt 1000000 ]
do
    echo $count
     count=$(($count+1))
    rm -rf .dropbox.cache/
    sleep 500
done
