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
import java.io.PrintStream;

/**
 * Implementation of an UploadHandler
 */
class MyUploadHandler implements MonetConnection.UploadHandler {
	private final long rows;
	private final long errorAt;
	private final String errorMessage;
	private boolean encounteredWriteError = false;
	private boolean cancelled = false;

	private int chunkSize = 100; // small number to trigger more bugs

	MyUploadHandler(long rows, long errorAt, String errorMessage) {
		this.rows = rows;
		this.errorAt = errorAt;
		this.errorMessage = errorMessage;
	}

	MyUploadHandler(long rows) {
		this(rows, -1, null);
	}

	MyUploadHandler(String errorMessage) {
		this(0, -1, errorMessage);
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	@Override
	public void uploadCancelled() {
		cancelled = true;
	}

	@Override
	public void handleUpload(MonetConnection.Upload handle, String name, boolean textMode, long linesToSkip) throws IOException {
		if (errorAt == -1 && errorMessage != null) {
			handle.sendError(errorMessage);
			return;
		}
		handle.setChunkSize(chunkSize);
		PrintStream stream = handle.getStream();
		for (long i = linesToSkip; i < rows; i++) {
			if (i == errorAt) {
				throw new IOException(errorMessage);
			}
			stream.printf("%d|%d%n", i + 1, i + 1);
			if (i % 25 == 0 && stream.checkError()) {
				encounteredWriteError = true;
				break;
			}
		}
	}

	public boolean encounteredWriteError() {
		return encounteredWriteError;
	}

	public boolean isCancelled() {
		return cancelled;
	}
}
