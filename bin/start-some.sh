#!/bin/bash

JAVA_OPTS="-Xms1024m -Xmx1024m"

bindir=$(cd `dirname $0`; pwd)

root=$(dirname ${bindir})

libdir=${root}"/target"
name=$(find ${libdir} -name "data-adaptor*.jar")

# if there are multi versions of destination jar file
names=$(echo $name | tr " " "\n")
# get the newest version and then run it
# newest version is just the largest version
if [ ${#names} -gt 1 ];
then
  mv=0

  for n in $names
  do
    version=$(echo $n | grep -Eo "([0-9]\.){0,2}[0-9]")
    i=0
    v=0
    while [ $i -lt ${#version} ];
    do
      vs=${version:$i:1}
      i=$[$i+1]
      if [ $vs != "." ];
      then
        v=$[$((10#${vs}+$v))]
      fi
    done
    if [ $v -gt $mv ];
    then
      mv=$v
      name=$n
    fi
  done

#  echo $name
fi

if [ $# -gt 0 ]; then
# echo $1
  java ${JAVA_OPTS} -jar ${name} $1
else
  java ${JAVA_OPTS} -jar ${name}
fi