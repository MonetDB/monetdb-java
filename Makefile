all: src/main/java/nl/cwi/monetdb/jdbc/MonetDriver.java
	ant -f build.xml distjdbc distmerocontrol
	cd tests; ant -f build.xml jar_jdbctests

src/main/java/nl/cwi/monetdb/jdbc/MonetDriver.java: build.properties src/main/java/nl/cwi/monetdb/jdbc/MonetDriver.java.in
	. ./build.properties; sed -e "s/@MCL_MAJOR@/$$MCL_MAJOR/g;s/@MCL_MINOR@/$$MCL_MINOR/g;s/@JDBC_MAJOR@/$$JDBC_MAJOR/g;s/@JDBC_MINOR@/$$JDBC_MINOR/g;s/@JDBC_VER_SUFFIX@/$$JDBC_VER_SUFFIX $$buildno/g;s/@JDBC_DEF_PORT@/$$JDBC_DEF_PORT/g" src/main/java/nl/cwi/monetdb/jdbc/MonetDriver.java.in > src/main/java/nl/cwi/monetdb/jdbc/MonetDriver.java

doc:
	ant -f build.xml doc

clean:
	rm -f src/main/java/nl/cwi/monetdb/jdbc/MonetDriver.java
	rm -rf build tests/build jars doc
