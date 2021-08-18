package org.monetdb.jdbc;

import org.monetdb.mcl.net.MapiSocket;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class MonetUploadHandle {
	private final MapiSocket server;
	private PrintStream print = null;
	private String error = null;

	MonetUploadHandle(MapiSocket server) {
		this.server = server;
	}

	public void sendError(String errorMessage) throws IOException {
		if (error != null) {
			throw new IOException("another error has already been sent: " + error);
		}
		error = errorMessage;
	}

	public PrintStream getStream() throws IOException {
		if (error != null) {
			throw new IOException("Cannot send data after an error has been sent");
		}
		if (print == null) {
			try {
				MapiSocket.UploadStream up = server.uploadStream();
				print = new PrintStream(up, false, "UTF-8");
				up.write('\n');
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException("The system is guaranteed to support the UTF-8 encoding but apparently it doesn't", e);
			}
		}
		return print;
	}

	public boolean hasBeenUsed() {
		return print != null || error != null;
	}

	public String getError() {
		return error;
	}

	public void uploadFrom(InputStream inputStream) throws IOException {
		OutputStream s = getStream();
		byte[] buffer = new byte[64 * 1024];
		while (true) {
			int nread = inputStream.read(buffer);
			if (nread < 0) {
				break;
			}
			s.write(buffer, 0, nread);
		}
	}

	public void uploadFrom(BufferedReader reader, int offset) throws IOException {
		// we're 1-based but also accept 0
		if (offset > 0) {
			offset -= 1;
		}

		for (int i = 0; i < offset; i++) {
			String line = reader.readLine();
			if (line == null) {
				return;
			}
		}

		uploadFrom(reader);
	}

	public void uploadFrom(BufferedReader reader) throws IOException {
		OutputStream s = getStream();
		OutputStreamWriter writer = new OutputStreamWriter(s, StandardCharsets.UTF_8);
		char[] buffer = new char[64 * 1024];
		while (true) {
			int nread = reader.read(buffer, 0, buffer.length);
			if (nread < 0) {
				break;
			}
			writer.write(buffer, 0, nread);
			writer.close();
		}
	}

	public void close() {
		if (print != null) {
			print.close();
		}
	}
}
