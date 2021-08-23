package org.monetdb.jdbc;

import java.io.IOException;

public interface MonetUploadHandler {
	void handleUpload(MonetConnection.Upload handle, String name, boolean textMode, int offset) throws IOException;
}
