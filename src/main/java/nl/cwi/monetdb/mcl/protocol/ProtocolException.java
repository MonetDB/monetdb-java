/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.protocol;

import java.text.ParseException;

/**
 * When an ProtocolException is thrown, the underlying protocol is violated by the sender. In general a stream reader
 * throws an ProtocolException as soon as something that is read cannot be understood or does not conform to the
 * specifications (e.g. a missing field). The instance that throws the exception will try to give an error offset
 * whenever possible. Alternatively it makes sure that the error message includes the offending data read.
 *
 * @author Fabian Groffen
 */
public class ProtocolException extends ParseException {

	private static final long serialVersionUID = 1L;

	public ProtocolException(String e) {
		super(e, -1);
	}

	public ProtocolException(String e, int offset) {
		super(e, offset);
	}
}
