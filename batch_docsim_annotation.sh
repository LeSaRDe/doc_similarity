#!/bin/bash

while IFS='' read -r line || [[ -n "$line" ]]; do
    echo "Annotating user: $line"
#	java -Xms20480m usersimproj.AnnotateUserText $1 2015-01-01T00:00:00Z 2019-09-01T00:00:00Z
	./annotation_docsim.sh $line &
	wait	
done < "$1"

