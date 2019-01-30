#!/bin/bash

while IFS='' read -r line || [[ -n "$line" ]]; do
    echo "Config: $line"
    user_1="$(cut -d'|' -f1 <<< $line)"
    user_2="$(cut -d'|' -f2 <<< $line)"
    t_1_s="$(cut -d'|' -f3 <<< $line)"
    t_1_e="$(cut -d'|' -f4 <<< $line)"
    t_2_s="$(cut -d'|' -f5 <<< $line)"
    t_2_e="$(cut -d'|' -f6 <<< $line)"
    sp="_"
    output_file="/home/fcmeng/doc_sim/ret_nasari/docsim_ret_nasari"
    #output_file=$output_file$sp$user_1$sp$user_2
    #sbatch -W doc_sim_run_nasari.sh $user_1 $user_2 $t_1_s $t_1_e $t_2_s $t_2_e $output_file
    { ./doc_sim_run_nasari.sh $user_1 $user_2 $t_1_s $t_1_e $t_2_s $t_2_e $output_file; } &
    wait;
done < "$1"
