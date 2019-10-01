#!/bin/bash

# User specific aliases and functions
# DO NOT change the classpath order!!!

export CLASSPATH="$CLASSPATH:.:./bin/:./config/"
for file in `find /home/$USER/workspace/lib  -name "*.jar"`; do export CLASSPATH="$CLASSPATH:`realpath $file`"; done 
#for file in `find /home/fcmeng/babel/lucene-7.4.0 -name "*.jar"`; do export CLASSPATH="$CLASSPATH:`realpath $file`"; done
#for file in `find /home/fcmeng/babel/Babelfy-online-API-1.0 -name "*.jar"`; do export CLASSPATH="$CLASSPATH:`realpath $file`:/home/fcmeng/babel/Babelfy-online-API-1.0/config"; done
#for file in `find /home/fcmeng/babel/BabelNet-API-4.0.1 -name "*.jar"`; do export CLASSPATH="$CLASSPATH:`realpath $file`:/home/fcmeng/babel/BabelNet-API-4.0.1/config"; done
#export CLASSPATH="$CLASSPATH:/home/fcmeng/adw/ADW-master/jar/*"
export _JAVA_OPTIONS="-Djava.net.preferIPv4Stack=true"
