#!/usr/bin/env bash

# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024, 2025 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.

if [[ -z $1 ]] ; then
	echo "Usage: $0 [-w] <(major|minor)=newversion> [...]"
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

CUR_MAJOR=$(eval "get_value 'JDBC_MAJOR'")
CUR_MINOR=$(eval "get_value 'JDBC_MINOR'")

NEW_MAJOR=${CUR_MAJOR}
NEW_MINOR=${CUR_MINOR}

ESC_MAJOR=$(escape_value ${CUR_MAJOR})
ESC_MINOR=$(escape_value ${CUR_MINOR})

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
	esac
done

echo "Current version: ${CUR_MAJOR}.${CUR_MINOR}"
echo "New version:     ${NEW_MAJOR}.${NEW_MINOR}"

diff="diff -Naur"

file="release.txt"
sed \
	-e "s|version ${ESC_MAJOR}\.${ESC_MINOR}|version ${NEW_MAJOR}.${NEW_MINOR}|g" \
	-e "s|JDBC-${ESC_MAJOR}\.${ESC_MINOR}|JDBC-${NEW_MAJOR}.${NEW_MINOR}|g" \
	-e "s|Release date: 20[0-9][0-9]-[01][0-9]-[0-3][0-9]|Release date: `date +%F`|" \
	${file} | ${diff} ${file} - | ${patch}

file="build.properties"
sed \
	-e "s|JDBC_MAJOR=${ESC_MAJOR}|JDBC_MAJOR=${NEW_MAJOR}|g" \
	-e "s|JDBC_MINOR=${ESC_MINOR}|JDBC_MINOR=${NEW_MINOR}|g" \
	${file} | ${diff} ${file} - | ${patch}

file="pom.xml"
sed \
	-e "/monetdb-jdbc/,/MonetDB JDBC driver/s|<version>${ESC_MAJOR}\.${ESC_MINOR}</version>|<version>${NEW_MAJOR}.${NEW_MINOR}</version>|g" \
	${file} | ${diff} ${file} - | ${patch}
