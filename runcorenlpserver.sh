#!/bin/bash

java edu.stanford.nlp.pipeline.StanfordCoreNLPServer -port $1 -timeout 600000 -annotators tokenize, ssplit, pos, lemma, ner, parse
