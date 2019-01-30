#!/bin/bash

java edu.stanford.nlp.pipeline.StanfordCoreNLPServer -port 9000 -timeout 600000 -serverProperties /hpchome/fcmeng/babel/stanford-corenlp-full-2018-02-27/usersimproj.properties
