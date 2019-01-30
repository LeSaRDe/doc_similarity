#!/bin/bash

echo "working on $1 ..."
java -Xms20480m usersimproj.AnnotateUserText $1 2015-01-01T00:00:00Z 2019-09-01T00:00:00Z
#sleep 2
