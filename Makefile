all: src/main/java/org/monetdb/jdbc/MonetVersion.java
	ant -f build.xml distjdbc distmerocontrol
	cd tests; ant -f build.xml jar_jdbctests

jre17jars: src/main/java/org/monetdb/jdbc/MonetVersion.java
	rm -rf build
	ant -f build_jre17.xml distjdbc
	rm -rf build

src/main/java/org/monetdb/jdbc/MonetVersion.java: build.properties src/main/java/org/monetdb/jdbc/MonetVersion.java.in
	. ./build.properties; sed -e "s/@JDBC_MAJOR@/$$JDBC_MAJOR/g;s/@JDBC_MINOR@/$$JDBC_MINOR/g;s/@JDBC_VER_SUFFIX@/$$JDBC_VER_SUFFIX $$buildno/g" src/main/java/org/monetdb/jdbc/MonetVersion.java.in > src/main/java/org/monetdb/jdbc/MonetVersion.java

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

