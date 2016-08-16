#!/bin/bash
count=0


while [ $count -lt 1000000 ]
do
    echo $count
    count=$(($count+1))
	git commit -a -m "auto commit `date +%F-%T`"
    sleep 4000
done
