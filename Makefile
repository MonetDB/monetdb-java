#

# Build the main jar. Suppress sources jar, docs jar and all tests.
default:
	./mvnw -Pquick -DskipTests package

# Run tests but not the slow ones
test:
	# Run up to phase 'test'. Skip the slow tests
	./mvnw -Pquick test

# Run all tests including the slow ones.
testall:
	./mvnw test

clean:
	./mvnw clean



