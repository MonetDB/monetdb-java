package org.monetdb.mcl.net;

public class ValidationError extends Exception {
	public ValidationError(String parameter, String message) {
		super(parameter + ": " + message);
	}

	public ValidationError(String message) {
		super(message);
	}
}
