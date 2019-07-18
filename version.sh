#!/usr/bin/env bash

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.

if [[ -z $1 ]] ; then
	echo "Usage: $0 [-w] <(jdbc|mcl)> <(major|minor|suffix|snapshot)=newversion> [...]"
	echo "where -w activates actual write of changes"
	exit -1
fi

PROPERTIES='build.properties'

get_value() {
	local tmp=$(grep -E "^$*=" ${PROPERTIES})
	echo ${tmp#*=}
}

escape_value() {
	echo "$*" | sed -e 's/\*/\\*/g' -e 's/\./\\./g'
}

patch="cat"

# get rid of the script name
case $1 in
	-w)
		patch="patch -p0";
		shift
		;;
esac
case $1 in
	jdbc)
		TYPE=JDBC
		FILES="monetdb-jdbc-XXX.jar"
		;;
	mcl)
		TYPE=MCL
		FILES="monetdb-mcl-XXX.jar"
		;;
	*)
		echo "invalid type: $1"
		exit -1
		;;
esac
shift

CUR_MAJOR=$(eval "get_value '${TYPE}_MAJOR'")
CUR_MINOR=$(eval "get_value '${TYPE}_MINOR'")
CUR_SUFFIX=$(eval "get_value '${TYPE}_VER_SUFFIX'")
CUR_SNAPSHOT=$(eval "get_value '${TYPE}_SNAPSHOT'")

NEW_MAJOR=${CUR_MAJOR}
NEW_MINOR=${CUR_MINOR}
NEW_SUFFIX=${CUR_SUFFIX}
NEW_SNAPSHOT=${CUR_SNAPSHOT}

ESC_MAJOR=$(escape_value ${CUR_MAJOR})
ESC_MINOR=$(escape_value ${CUR_MINOR})
ESC_SUFFIX=$(escape_value ${CUR_SUFFIX})
ESC_SNAPSHOT=$(escape_value ${CUR_SNAPSHOT})

for param in $* ; do
	arg=${param%%=*}
	val=${param#*=}
	num=$(echo ${val} | grep -E '[0-9]+' -o | head -n1)
	case ${arg} in
	major)
		if [[ -z ${num} ]] ; then
			echo "major needs a numeric argument!";
			exit -1
		fi
		NEW_MAJOR=${num}
		;;
	minor)
		if [[ -z ${num} ]] ; then
			echo "minor needs a numeric argument!";
			exit -1
		fi
		NEW_MINOR=${num}
		;;
	suffix)
		NEW_SUFFIX=${val}
		;;
	snapshot)
		NEW_SNAPSHOT=${val}
		;;
	esac
done

echo "Current version: ${CUR_MAJOR}.${CUR_MINOR}${CUR_SNAPSHOT} (${CUR_SUFFIX})"
echo "New version:     ${NEW_MAJOR}.${NEW_MINOR}${NEW_SNAPSHOT} (${NEW_SUFFIX})"

diff="diff -Naur"

file="release.txt"
sed \
	-e "s|version ${ESC_MAJOR}\.${ESC_MINOR}${ESC_SNAPSHOT} (${ESC_SUFFIX}|version ${NEW_MAJOR}.${NEW_MINOR}${NEW_SNAPSHOT} \(${NEW_SUFFIX}|g" \
	-e "s|${TYPE}-${ESC_MAJOR}\.${ESC_MINOR}${ESC_SNAPSHOT}|${TYPE}-${NEW_MAJOR}.${NEW_MINOR}${NEW_SNAPSHOT}|g" \
	-e "s|Release date: 20[0-9][0-9]-[01][0-9]-[0-3][0-9]|Release date: `date +%F`|" \
	${file} | ${diff} ${file} - | ${patch}

file="build.properties"
sed \
	-e "s|${TYPE}_MAJOR=${ESC_MAJOR}|${TYPE}_MAJOR=${NEW_MAJOR}|g" \
	-e "s|${TYPE}_MINOR=${ESC_MINOR}|${TYPE}_MINOR=${NEW_MINOR}|g" \
	-e "s|${TYPE}_SNAPSHOT=${ESC_SNAPSHOT}|${TYPE}_SNAPSHOT=${NEW_SNAPSHOT}|g" \
	-e "s|${TYPE}_VER_SUFFIX=${ESC_SUFFIX}|${TYPE}_VER_SUFFIX=${NEW_SUFFIX}|g" \
	${file} | ${diff} ${file} - | ${patch}

file="pom.xml"
sed \
	-e "s|<version>${ESC_MAJOR}\.${ESC_MINOR}${ESC_SNAPSHOT}</version>|<version>${NEW_MAJOR}.${NEW_MINOR}${NEW_SNAPSHOT}</version>|g" \
	${file} | ${diff} ${file} - | ${patch}

file="upload_jdbc_new.sh"
sed \
	-e "s|monetdb-jdbc-new-${ESC_MAJOR}\.${ESC_MINOR}${ESC_SNAPSHOT}|monetdb-jdbc-new-${NEW_MAJOR}.${NEW_MINOR}${NEW_SNAPSHOT}|g" \
	${file} | ${diff} ${file} - | ${patch}
