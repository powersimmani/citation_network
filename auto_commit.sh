#!/bin/bash
count=0


while [ $count -lt 1000000 ]
do
    echo $count
    count=$(($count+1))
	git commit -a -m "auto commit `date +%F-%T`"
	echo "please push the project"
    sleep 3
done
