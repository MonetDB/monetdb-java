/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

package org.monetdb.util;

import org.monetdb.jdbc.MonetConnection;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Sample implementation of COPY ... INTO 'file-name' ON CLIENT handling
 *
 * Can be registered with {@link MonetConnection#setUploadHandler(MonetConnection.UploadHandler)}
 * and {@link MonetConnection#setDownloadHandler(MonetConnection.DownloadHandler)}.
 * Implements uploads and downloads by reading and writing files on the file system.
 */
public class FileTransferHandler implements MonetConnection.UploadHandler, MonetConnection.DownloadHandler {
	private final Path root;
	private final boolean utf8Encoded;

	/**
	 * Create a new FileTransferHandler which serves the given directory.
	 *
	 * @param dir directory to read and write files from
	 * @param utf8Encoded set this to true if all files in the directory are known to be utf-8 encoded.
	 */
	public FileTransferHandler(final Path dir, final boolean utf8Encoded) {
		root = dir.toAbsolutePath().normalize();
		this.utf8Encoded = utf8Encoded;
	}

	/**
	 * Create a new FileTransferHandler which serves the given directory.
	 *
	 * @param dir directory to read and write files from
	 * @param utf8Encoded set this to true if all files in the directory are known to be utf-8 encoded.
	 */
	public FileTransferHandler(final String dir, final boolean utf8Encoded) {
		this(FileSystems.getDefault().getPath(dir), utf8Encoded);
	}

	public void handleUpload(final MonetConnection.Upload handle, final String name, final boolean textMode, final long linesToSkip) throws IOException {
		final Path path = root.resolve(name).normalize();
		if (!path.startsWith(root)) {
			handle.sendError("File is not in upload directory");
			return;
		}
		if (!Files.isReadable(path)) {
			handle.sendError("Cannot read " + name);
			return;
		}
		if (textMode && (linesToSkip > 0 || !utf8Encoded)) {
			final Charset encoding = utf8Encoded ? StandardCharsets.UTF_8 : Charset.defaultCharset();
			handle.uploadFrom(Files.newBufferedReader(path, encoding), linesToSkip);
		} else {
			handle.uploadFrom(Files.newInputStream(path));
		}
	}

	public void handleDownload(final MonetConnection.Download handle, final String name, final boolean textMode) throws IOException {
		final Path path = root.resolve(name).normalize();
		if (!path.startsWith(root)) {
			handle.sendError("File is not in download directory");
			return;
		}
		if (Files.exists(path)) {
			handle.sendError("File already exists: " + name);
			return;
		}
		handle.downloadTo(Files.newOutputStream(path, StandardOpenOption.CREATE_NEW));
	}
}
