#!/usr/bin/perl

# Simple script to start a Node Manager that uses a compiled lib.jar

# Takes synoptic args, usually
# -s -n=<Node> -f <failure level> [-c script]

main();

sub main {
    
    $classpath = "proj/:jars/plume.jar:jars/lib.jar";
    
    $args = join " ", @ARGV;

    exec("java -cp $classpath edu.washington.cs.cse490h.lib.MessageLayer -l partial.log -L total.log $args 2>&1 | tee full_output.log");
}

