#!/bin/bash

# hand this -c <synoptic.args> <file.log>
# or any other synoptic command line args

java -jar jars/synoptic.jar -d /usr/bin/dot -f $*
