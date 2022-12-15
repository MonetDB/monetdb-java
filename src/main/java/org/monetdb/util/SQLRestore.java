/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

package org.monetdb.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.monetdb.mcl.io.BufferedMCLReader;
import org.monetdb.mcl.io.BufferedMCLWriter;
import org.monetdb.mcl.io.LineType;
import org.monetdb.mcl.net.MapiSocket;

/**
 * Use this class to restore an SQL dump file.
 */
public final class SQLRestore {

	private final String _host;
	private final int _port;
	private final String _user;
	private final String _password;
	private final String _dbName;

	public SQLRestore(final String host, final int port, final String user, final String password, final String dbName) throws IOException {
		if (host == null || user == null || password == null || dbName == null)
			throw new NullPointerException();
		_host = host;
		_port = port;
		_user = user;
		_password = password;
		_dbName = dbName;
	}

	private static final class ServerResponseReader implements Runnable {
		private final BufferedMCLReader _is;
		private final AtomicBoolean _errorState = new AtomicBoolean(false);
		private String _errorMessage = null;

		ServerResponseReader(final BufferedMCLReader is) {
			_is = is;
		}

		public void run() {
			try {
				while (true) {
					_is.advance();
					final String line = _is.getLine();
					if (line == null)
						break;
					final LineType result = _is.getLineType();
					switch (result) {
					case ERROR:
						_errorMessage = line;
						_errorState.set(true);
						return;
					default:
						// do nothing...
					}
				}
			} catch (IOException e) {
				_errorMessage = e.getMessage();
				_errorState.set(true);
			} finally {
				try {
					_is.close();
				} catch (IOException e) {
					// ignore errors
				}
			}
		}

		/**
		 * Check whether the server has responded with an error.
		 * Any error is regarded as fatal.
		 * @return whether the server has responded with an error
		 */
		public boolean inErrorState() {
			return _errorState.get();
		}

		/**
		 * Get error message if inErrorState() is true.
		 * Behaviour is not defined if called before inErrorState is true.
		 * @return the error message
		 */
		public String getErrorMessage() {
			return _errorMessage;
		}
	}

	/**
	 * Restores a given SQL dump to the database.
	 *
	 * @param source file object
	 * @throws IOException when IO exception occurred
	 */
	public void restore(final File source) throws IOException {
		final MapiSocket ms = new MapiSocket();
		try {
			ms.setLanguage("sql");
			ms.setDatabase(_dbName);
			ms.connect(_host, _port, _user, _password);

			final BufferedMCLWriter os = ms.getWriter();
			final BufferedMCLReader reader = ms.getReader();

			final ServerResponseReader srr = new ServerResponseReader(reader);

			final Thread responseReaderThread = new Thread(srr);
			responseReaderThread.start();
			try {
				// FIXME: we assume here that the dump is in system's default encoding
				final BufferedReader sourceData = new BufferedReader(new java.io.FileReader(source));
				try {
					os.write('s'); // signal that a new statement (or series of) is coming
					while(!srr.inErrorState()) {
						final char[] buf = new char[4096];
						final int result = sourceData.read(buf);
						if (result < 0)
							break;
						os.write(buf, 0, result);
					}

					os.flush(); // mark the end of the statement (or series of)
					os.close();
				} finally {
					sourceData.close();
				}
			} finally {
				try {
					responseReaderThread.join();
				} catch (InterruptedException e) {
					throw new IOException(e.getMessage());
				}

				// if the server signalled an error, we should respect it...
				if (srr.inErrorState()) {
					throw new IOException(srr.getErrorMessage());
				}
			}
		} catch (org.monetdb.mcl.MCLException e) {
			throw new IOException(e.getMessage());
		} catch (org.monetdb.mcl.parser.MCLParseException e) {
			throw new IOException(e.getMessage());
		} finally {
			ms.close();
		}
	}

	public void close() {
		// do nothing at the moment...
	}


	public static void main(String[] args) throws IOException {
		if (args.length != 6) {
			System.err.println("USAGE: java " + SQLRestore.class.getName() +
					" <host> <port> <user> <password> <dbname> <dumpfile>");
			System.exit(1);
		}

		// parse arguments
		final String host = args[0];
		final int port = Integer.parseInt(args[1]); // FIXME: catch NumberFormatException
		final String user = args[2];
		final String password = args[3];
		final String dbName = args[4];
		final File dumpFile = new File(args[5]);

		// check arguments
		if (!dumpFile.isFile() || !dumpFile.canRead()) {
			System.err.println("Cannot read: " + dumpFile);
			System.exit(1);
		}

		final SQLRestore md = new SQLRestore(host, port, user, password, dbName);
		try {
			System.out.println("Start restoring " + dumpFile);
			long duration = -System.currentTimeMillis();
			md.restore(dumpFile);
			duration += System.currentTimeMillis();
			System.out.println("Restoring took: " + duration + "ms");
		} finally {
			md.close();
		}
	}
}
