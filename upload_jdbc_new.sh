#!/bin/bash

set -e

function echo_and_exit {
	echo $1 >&2
	exit 1
}

if [[ -z $JAVA_HOME ]] ; then
    echo_and_exit "The JAVA_HOME directory must be set"
fi

# Prepare the directory to upload to our website
mkdir synchronizing
# Move the monetdb-jdbc-new jar
mv jars/monetdb-jdbc-new-2.37-SNAPSHOT.jar synchronizing/monetdb-jdbc-new-2.37-SNAPSHOT.jar
# Rsync the library files to the monet.org machine
rsync -aqz --ignore-times synchronizing/* ferreira@monetdb.org:/var/www/html/downloads/Java-Experimental/
# Remove it in the end
rm -rf synchronizing
