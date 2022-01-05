/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

package org.monetdb.util;

import org.monetdb.jdbc.MonetConnection;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Default implementation of UploadHandler and DownloadHandler interfaces
 * for reading from and writing to files on the local file system.
 * It enables support for:
 *   COPY .. INTO table FROM 'file-name' ON CLIENT ...
 * and
 *   COPY SELECT_query INTO 'file-name' ON CLIENT ...
 * handling.
 *
 * Currently only file compression format .gz is supported. This is intentionally
 * as other compression formats would introduce dependencies on external
 * libraries which complicates usage of JDBC driver or JdbcClient application.
 * Developers can of course build their own MyFileTransferHandler class
 * and use it instead of this default implementation.
 *
 * A FileTransferHandler object needs to be registered via
 * {@link MonetConnection#setUploadHandler(MonetConnection.UploadHandler)} and/or
 * {@link MonetConnection#setDownloadHandler(MonetConnection.DownloadHandler)}.
 *
 * @author Joeri van Ruth
 * @author Martin van Dinther
 * @version 1.1
 */
public class FileTransferHandler implements MonetConnection.UploadHandler, MonetConnection.DownloadHandler {
	private final Path root;
	private final Charset encoding;

	/**
	 * Create a new FileTransferHandler which serves the given directory.
	 *
	 * @param dir directory Path to read and write files from
	 * @param encoding the specified characterSet encoding is used for all data files in the directory
	 *                 when null the Charset.defaultCharset() is used.
	 */
	public FileTransferHandler(final Path dir, final Charset encoding) {
		this.root = dir.toAbsolutePath().normalize();
		this.encoding = encoding != null ? encoding: Charset.defaultCharset();
	}

	/**
	 * Create a new FileTransferHandler which serves the given directory.
	 *
	 * @param dir directory String to read and write files from
	 * @param encoding the specified characterSet encoding is used for all data files in the directory
	 *                 when null the Charset.defaultCharset() is used.
	 */
	public FileTransferHandler(final String dir, final Charset encoding) {
		this(FileSystems.getDefault().getPath(dir), encoding);
	}

	/**
	 * Read the data from the specified file (in the root directory) and upload it to the server.
	 */
	public void handleUpload(final MonetConnection.Upload handle, final String name, final boolean textMode, final long linesToSkip) throws IOException {
		if (name == null || name.isEmpty()) {
			handle.sendError("Missing file name");
			return;
		}
		final Path path = root.resolve(name).normalize();
		if (!path.startsWith(root)) {
			handle.sendError("File is not in upload directory: " + root.toString());
			return;
		}
		if (!Files.isReadable(path)) {
			handle.sendError("Cannot read file " + path.toString());
			return;
		}

		// In this implementation we ONLY support gzip compression format and none of the other compression formats.
		if (name.endsWith(".bz2") || name.endsWith(".lz4") || name.endsWith(".xz") || name.endsWith(".zip")) {
			final String extension = name.substring(name.lastIndexOf('.'));
			handle.sendError("Specified file compression format " + extension + " is not supported. Only .gz is supported.");
			return;
		}

		InputStream byteStream = Files.newInputStream(path);
		if (name.endsWith(".gz")) {
			byteStream = new GZIPInputStream(byteStream, 128 * 1024);
		}

		if (!textMode || (linesToSkip == 0 && utf8Encoded())) {
			// when !textMode we must upload as a byte stream
			// when utf8Encoded and linesToSkip is 0 it is more efficient to upload as a byte stream
			handle.uploadFrom(byteStream);
			byteStream.close();
		} else {
			// cannot upload as a byte stream, must deal with encoding and/or linesToSkip
			final BufferedReader reader = new BufferedReader(new InputStreamReader(byteStream, encoding));
			handle.uploadFrom(reader, linesToSkip);
			reader.close();
		}
	}

	/**
	 * Download the data from the server and write it to a new created file in the root directory.
	 * When a file with the same name already exists the download request will send an error and NOT overwrite the existing file.
	 */
	public void handleDownload(final MonetConnection.Download handle, final String name, final boolean textMode) throws IOException {
		if (name == null || name.isEmpty()) {
			handle.sendError("Missing file name");
			return;
		}
		final Path path = root.resolve(name).normalize();
		if (!path.startsWith(root)) {
			handle.sendError("File is not in download directory: " + root.toString());
			return;
		}
		if (Files.exists(path)) {
			handle.sendError("File already exists: " + path.toString());
			return;
		}

		// In this implementation we ONLY support gzip compression format and none of the other compression formats.
		if (name.endsWith(".bz2") || name.endsWith(".lz4") || name.endsWith(".xz") || name.endsWith(".zip")) {
			final String extension = name.substring(name.lastIndexOf('.'));
			handle.sendError("Requested file compression format " + extension + " is not supported. Use .gz instead.");
			return;
		}

		OutputStream byteStream = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW);
		if (name.endsWith(".gz")) {
			byteStream = new GZIPOutputStream(byteStream, 128 * 1024);
		}

		if (!textMode || utf8Encoded()) {
			// when !textMode we must download as a byte stream
			// when utf8Encoded it is more efficient to download as a byte stream
			handle.downloadTo(byteStream);
			byteStream.close();
		} else {
			// cannot download as a byte stream, must deal with encoding
			final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(byteStream, encoding));
			handle.downloadTo(writer);
			writer.close();
		}
	}

	public boolean utf8Encoded() {
		return encoding.equals(StandardCharsets.UTF_8);
	}
}
