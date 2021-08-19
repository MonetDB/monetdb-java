package org.monetdb.jdbc;

import java.io.IOException;

public interface MonetUploader {
	void handleUpload(MonetUploadHandle handle, String name, boolean textMode, int offset) throws IOException;
}
