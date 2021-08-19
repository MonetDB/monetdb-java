package org.monetdb.jdbc;

import org.monetdb.mcl.net.MapiSocket;

import java.io.*;

public class MonetDownloadHandle {
	private final MapiSocket server;
	private String error = null;

	MonetDownloadHandle(MapiSocket server) {
		this.server = server;
	}

	public void sendError(String errorMessage) throws IOException {
		if (error != null) {
			throw new IOException("another error has already been sent: " + error);
		}
		error = errorMessage;
	}

	public boolean hasBeenUsed() {
		return error != null;
	}

	public String getError() {
		return error;
	}

	public void close() {
	}
}
