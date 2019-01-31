#!/bin/bash

java edu.stanford.nlp.pipeline.StanfordCoreNLPServer -port $1 -timeout 600000 -serverProperties ./config/usersimproj.properties
