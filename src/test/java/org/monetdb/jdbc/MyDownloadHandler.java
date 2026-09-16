/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */
package org.monetdb.jdbc;

import java.io.IOException;
import java.io.InputStream;

/**
 * Implementation of a DownloadHandler
 */
class MyDownloadHandler implements MonetConnection.DownloadHandler {
	private final int errorAtByte;
	private final String errorMessage;
	private int attempts = 0;
	private int bytesSeen = 0;
	private int lineEndingsSeen = 0;
	private int startOfLine = 0;

	MyDownloadHandler(int errorAtByte, String errorMessage) {
		this.errorAtByte = errorAtByte;
		this.errorMessage = errorMessage;
	}

	MyDownloadHandler(String errorMessage) {
		this(-1, errorMessage);
	}

	MyDownloadHandler() {
		this(-1, null);
	}

	@Override
	public void handleDownload(MonetConnection.Download handle, String name, boolean textMode) throws IOException {
		attempts++;
		bytesSeen = 0;
		lineEndingsSeen = 0;
		startOfLine = 0;

		if (errorMessage != null && errorAtByte < 0) {
			handle.sendError(errorMessage);
			return;
		}

		InputStream stream = handle.getStream();
		byte[] buffer = new byte[1024];
		while (true) {
			int toRead = buffer.length;
			if (errorMessage != null && errorAtByte >= 0) {
				if (bytesSeen == errorAtByte) {
					throw new IOException(errorMessage);
				}
				toRead = Integer.min(toRead, errorAtByte - bytesSeen);
			}
			int nread = stream.read(buffer, 0, toRead);
			if (nread < 0)
				break;
			for (int i = 0; i < nread; i++) {
				if (buffer[i] == '\n') {
					lineEndingsSeen += 1;
					startOfLine = bytesSeen + i + 1;
				}
			}
			bytesSeen += nread;
		}
	}

	public int countAttempts() {
		return attempts;
	}

	public int lineCount() {
		int lines = lineEndingsSeen;
		if (startOfLine != bytesSeen)
			lines++;
		return lines;
	}
}
