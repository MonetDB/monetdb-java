/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */
package org.monetdb.testinfra;

import java.io.BufferedOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Prefix every line of the output with "# "
 */
public class PrefixedOutputStream extends FilterOutputStream {
	private static final byte[] PREFIX = new byte[]{ '#', ' '};
	boolean atStart = true;

	public PrefixedOutputStream(OutputStream out) {
		super(new BufferedOutputStream(out));
	}

	@Override
	public void write(int b) throws IOException {
		if (atStart) {
			out.write(PREFIX);
			atStart = false;
		}
		out.write(b);
		if ( b == '\n') {
			atStart = true;
			out.flush();
		}
	}

	@Override
	public void write(byte[] bytes, int off, int len) throws IOException {
		for (int i = off; i < len; i++)
			this.write(bytes[i]);
	}

	@Override
	public void write(byte[] b) throws IOException {
		this.write(b, 0, b.length);
	}
}
