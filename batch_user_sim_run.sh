#!/bin/bash

while IFS='' read -r line || [[ -n "$line" ]]; do
    echo "Config: $line"
    user_1="$(cut -d'|' -f1 <<< $line)"
    user_2="$(cut -d'|' -f2 <<< $line)"
    t_1_s="$(cut -d'|' -f3 <<< $line)"
    t_1_e="$(cut -d'|' -f4 <<< $line)"
    t_2_s="$(cut -d'|' -f5 <<< $line)"
    t_2_e="$(cut -d'|' -f6 <<< $line)"
    output_file="/home/fcmeng/user_similarity_project/user_sim_ret_top50avg"
    sbatch user_sim_run_pagasus.sh $user_1 $user_2 $t_1_s $t_1_e $t_2_s $t_2_e $output_file
    #{ ./user_sim_run.sh $user_1 $user_2 $t_1_s $t_1_e $t_2_s $t_2_e $output_file; } &
    #wait;
done < "$1"
