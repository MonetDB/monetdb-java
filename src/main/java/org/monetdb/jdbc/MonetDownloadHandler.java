package org.monetdb.jdbc;

import java.io.IOException;

public interface MonetDownloadHandler {
	void handleDownload(MonetConnection.Download handle, String name, boolean textMode) throws IOException;
}
