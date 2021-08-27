package org.monetdb.jdbc;

import java.io.IOException;

/**
 * Callback for receiving files with COPY ON CLIENT
 *
 * To be registered with {@link MonetConnection#setDownloadHandler(MonetDownloadHandler)}
 */
public interface MonetDownloadHandler {
	/**
	 * Called if the server sends a request to write a file.
	 * 
	 * Use the given handle to send data or errors to the server.
	 * 
	 * @param handle Handle to communicate with the server
	 * @param name Name of the file the server would like to write. Make sure to validate this before writing to
	 *             the file system
	 * @param textMode Whether this is text or binary data.
	 */
	void handleDownload(MonetConnection.Download handle, String name, boolean textMode) throws IOException;
}