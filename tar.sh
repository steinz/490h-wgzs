#!/bin/bash

OUTPUT_FILE="cse490h-wgzs.tar.gz"
SOURCE="cse490h-wgzs/proj/edu/washington/cs/ces490h/tdfs/*.java cse490h-wgzs/tests/*.java"
TEST="cse490h-wgzs/tests/edu/washington/cs/cse490h/tdfs/tests/*.java"
LIB="cse490h-wgzs/jars/*.jar cse490h-wgzs/jars/lib/*.jar"
SCRIPTS="cse490h-wgzs/*.sh cse490h-wgzs/*.pl cse490h-wgzs/*.py"
WRITEUPS="cse490h-wgzs/writeups/project*/*.pdf cse490h-wgzs/writeups/project*/*.png"

tar -cvzf $OUTPUT_FILE $SOURCE $LIB $SCRIPTS $WRITEUPS 