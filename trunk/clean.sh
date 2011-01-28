#!/bin/bash

rm server_log.txt partial.log total.log 2> /dev/null
rm -rf storage/* 2> /dev/null
rm output/*.dot output/*.png 2> /dev/null