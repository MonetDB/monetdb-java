all: src/main/java/org/monetdb/jdbc/MonetVersion.java.in
	ant -f build.xml distjdbc distmerocontrol
	cd tests; ant -f build.xml jar_jdbctests

jre17jars: src/main/java/org/monetdb/jdbc/MonetVersion.java
	rm -rf build
	ant -f build_jre17.xml distjdbc
	rm -rf build

jre21jars: src/main/java/org/monetdb/jdbc/MonetVersion.java
	rm -rf build
	ant -f build_jre21.xml distjdbc
	rm -rf build

test: all
	echo banana
	cd tests; ant  -f build.xml test

testsjar:
	cd tests; ant -f build.xml jar_jdbctests

doc:
	ant -f build.xml doc

clean:
	rm -f src/main/java/org/monetdb/jdbc/MonetVersion.java
	rm -rf build tests/build jars doc

cleandoc:
	rm -rf doc

cleantests:
	rm -rf tests/build
	rm -f  jars/jdbctests.jar

