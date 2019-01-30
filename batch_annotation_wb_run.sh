#!/bin/bash

for file in /home/fcmeng/gh_data/ann_ret/*; do
    echo "Write back $file ..."
    { python /home/fcmeng/user_similarity_project/src/usersimproj/annotation_update.py $file; } &
    wait;
done
