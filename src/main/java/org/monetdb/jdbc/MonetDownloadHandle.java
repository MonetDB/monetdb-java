package org.monetdb.jdbc;

import org.monetdb.mcl.net.MapiSocket;

import java.io.*;

public class MonetDownloadHandle {
	private final MapiSocket server;
	private MapiSocket.DownloadStream stream = null;
	private String error = null;
	boolean closed = false;

	MonetDownloadHandle(MapiSocket server) {
		this.server = server;
	}

	public void sendError(String errorMessage) throws IOException {
		if (error != null) {
			throw new IOException("another error has already been sent: " + error);
		}
		error = errorMessage;
	}

	public InputStream getStream() throws IOException {
		if (error != null) {
			throw new IOException("cannot receive data after error has been sent");
		}
		if (stream == null) {
			stream = server.downloadStream();
			server.getOutputStream().flush();
		}
		return stream;
	}

	public boolean hasBeenUsed() {
		return error != null || stream != null;
	}

	public String getError() {
		return error;
	}

	public void close() throws IOException {
		if (closed) {
			return;
		}
		if (stream != null) {
			stream.close();
		}
		closed = true;
	}
}
