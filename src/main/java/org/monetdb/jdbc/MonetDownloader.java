package org.monetdb.jdbc;

import java.io.IOException;

public interface MonetDownloader {
	void handleDownload(MonetDownloadHandle handle, String name, boolean textMode) throws IOException;
}
