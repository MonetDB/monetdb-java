package org.monetdb.jdbc;

import java.io.IOException;

/**
 * Callback for sending files for COPY ON CLIENT
 *
 * To be registered with {@link MonetConnection#setUploadHandler(MonetUploadHandler)}
 */

public interface MonetUploadHandler {
	/**
	 * Called if the server sends a request to write a file.
	 *
	 * Use the given handle to receive data or send errors to the server.
	 *
	 * @param handle Handle to communicate with the server
	 * @param name Name of the file the server would like to read. Make sure to validate this before reading from
	 *             the file system
	 * @param textMode Whether this is text or binary data.
	 */
	void handleUpload(MonetConnection.Upload handle, String name, boolean textMode, int offset) throws IOException;
}