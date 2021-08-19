package org.monetdb.util;

import org.monetdb.jdbc.MonetUploader;
import org.monetdb.jdbc.MonetUploadHandle;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileTransfer implements MonetUploader {
	private final Path root;
	private final boolean utf8Encoded;

	public FileTransfer(Path dir, boolean utf8Encoded) {
		root = dir.toAbsolutePath().normalize();
		this.utf8Encoded = utf8Encoded;
	}

	public FileTransfer(String dir, boolean utf8Encoded) {
		this(FileSystems.getDefault().getPath(dir), utf8Encoded);
	}

	public void handleUpload(MonetUploadHandle handle, String name, boolean textMode, int offset) throws IOException {
		Path path = root.resolve(name).normalize();
		if (!path.startsWith(root)) {
			handle.sendError("File is not in upload directory");
			return;
		}
		if (!Files.isReadable(path)) {
			handle.sendError("Cannot read " + name);
			return;
		}
		if (textMode && (offset > 1 || !utf8Encoded)) {
			Charset encoding = utf8Encoded ? StandardCharsets.UTF_8 : Charset.defaultCharset();
			BufferedReader reader = Files.newBufferedReader(path, encoding);
			int toSkip = offset > 1 ? offset - 1 : 0;
			for (int i = 0; i < toSkip; i++) {
				reader.readLine();
			}
			handle.uploadFrom(reader);
		} else {
			handle.uploadFrom(Files.newInputStream(path));
		}
	}
}
