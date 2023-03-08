all: src/main/java/org/monetdb/jdbc/MonetDriver.java
	ant -f build.xml distjdbc distmerocontrol
	cd tests; ant -f build.xml jar_jdbctests

jre17jars: src/main/java/org/monetdb/jdbc/MonetDriver.java
	rm -rf build
	ant -f build_jre17.xml distjdbc
	rm -rf build

src/main/java/org/monetdb/jdbc/MonetDriver.java: build.properties src/main/java/org/monetdb/jdbc/MonetDriver.java.in
	. ./build.properties; sed -e "s/@JDBC_MAJOR@/$$JDBC_MAJOR/g;s/@JDBC_MINOR@/$$JDBC_MINOR/g;s/@JDBC_VER_SUFFIX@/$$JDBC_VER_SUFFIX $$buildno/g;s/@JDBC_DEF_PORT@/$$JDBC_DEF_PORT/g" src/main/java/org/monetdb/jdbc/MonetDriver.java.in > src/main/java/org/monetdb/jdbc/MonetDriver.java

testsjar:
	cd tests; ant -f build.xml jar_jdbctests

doc:
	ant -f build.xml doc

clean:
	rm -f src/main/java/org/monetdb/jdbc/MonetDriver.java
	rm -rf build tests/build jars doc

cleandoc:
	rm -rf doc

cleantests:
	rm -rf tests/build
	rm -f  jars/jdbctests.jar

