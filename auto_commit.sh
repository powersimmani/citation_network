#!/bin/bash
count=0
git commit -a -m "auto commit `date +%F-%T`"
git push

#while [ $count -lt 1000000 ]
#do
#    echo $count
#     count=$(($count+1))
#    git add *
#    git commit -m "auto_commit_2016_08_16 16:33"
#    sleep 2000
#done
