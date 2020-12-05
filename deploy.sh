#!/bin/sh

function barf()
{
    local rc="$1"
    shift
    echo "$@"
    exit "$rc"
}

HADOOP_SSH="hadoop"

file="$1"

[ -z "$file" ] && barf 1 "using: ${0##*/} <file>"

echo "Sending "$file" to hadoop vm"
scp "$file" "$HADOOP_SSH":~/projects/

