#!/bin/bash

declare -a folder_arr=("201504" "201505" "201506" "201507" "201508" "201509" "201510" "201511" "201512" "201601" "201602" "201603" "201604" "201605" "201606" "201607")
for folder in "${folder_arr[@]}"
do
	echo "$folder"
	sbatch --exclusive text_run.sh $folder
done

