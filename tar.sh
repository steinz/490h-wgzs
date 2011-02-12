#!/bin/bash

OUTPUT_FILE="cse490h-wgzs.tar.gz"
SOURCE="cse490h-wgzs/proj/*.java cse490h-wgzs/tests/*.java"
LIB="cse490h-wgzs/jars/*.jar cse490h-wgzs/jars/lib/*.jar"
SCRIPTS="cse490h-wgzs/*.sh cse490h-wgzs/*.pl cse490h-wgzs/*.py""
SIM_SCRIPTS="cse490h-wgzs/simulator_scripts/Cache* cse490h-wgzs/simulator_scripts/test*"
WRITEUPS="cse490h-wgzs/writeups/project*/*.pdf cse490h-wgzs/writeups/project*/*.png"
OUTPUT=""
#OUTPUT="cse490h-wgzs/output/*/*.png"
SYNOPTIC="cse490h-wgzs/synoptic_args/*.args"

tar -cvzf $OUTPUT_FILE $SOURCE $LIB $SCRIPTS $SIM_SCRIPTS $WRITEUPS $OUTPUT $SYNOPTIC