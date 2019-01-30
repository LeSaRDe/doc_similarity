#!/bin/bash

i=0
while [ $i -le 5 ]
do
    {
        sleep 100;
        echo "task $i"
    }
    i=$((i+1))
done
echo "all done"
