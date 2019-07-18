/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.protocol.oldmapi;

import nl.cwi.monetdb.mcl.protocol.ProtocolException;
import nl.cwi.monetdb.mcl.protocol.StarterHeaders;

/**
 * The OldMapiStartOfHeaderParser is responsible to retrieve the server's headers on a SOHEADER response. Depending on
 * the type of the header, the next tokens should be retrieved as integers or Strings.
 *
 * @author Fabian Groffen, Pedro Ferreira
 */
final class OldMapiStartOfHeaderParser {

	private OldMapiStartOfHeaderParser() {}

	static int getNextStartHeaderOnOldMapi(OldMapiProtocol protocol) {
		int res;
		switch (protocol.lineBuffer.get()) {
			case '0':
				res = StarterHeaders.Q_PARSE;
				break;
			case '1':
				res = StarterHeaders.Q_TABLE;
				protocol.lineBuffer.get();
				break;
			case '2':
				res = StarterHeaders.Q_UPDATE;
				protocol.lineBuffer.get();
				break;
			case '3':
				res = StarterHeaders.Q_SCHEMA;
				break;
			case '4':
				res = StarterHeaders.Q_TRANS;
				protocol.lineBuffer.get();
				break;
			case '5':
				res = StarterHeaders.Q_PREPARE;
				protocol.lineBuffer.get();
				break;
			case '6':
				res = StarterHeaders.Q_BLOCK;
				protocol.lineBuffer.get();
				break;
			default:
				res = StarterHeaders.Q_UNKNOWN;
		}
		return res;
	}

	/**
	 * Returns the next token in the Protocol's lineBuffer as an integer. The value is considered to end at the end of
	 * the lineBuffer or at a space. If a non-numeric character is encountered a ProtocolException is thrown.
	 *
	 * @param protocol An Old Mapi Protocol instance where the next token will be retrieved
	 * @return The next token in the Protocol as an integer
	 * @throws ProtocolException if no numeric value could be read
	 */
	static int getNextResponseDataAsInt(OldMapiProtocol protocol) throws ProtocolException {
		int currentPointer = protocol.lineBuffer.position();
		int limit = protocol.lineBuffer.limit();
		char[] array = protocol.lineBuffer.array();

		if (currentPointer >= limit) {
			throw new ProtocolException("unexpected end of string", currentPointer - 1);
		}
		int tmp = 0;
		boolean positive = true;
		char chr = array[currentPointer++];
		// note: don't use Character.isDigit() here, because we only want ISO-LATIN-1 digits
		if (chr >= '0' && chr <= '9') {
			tmp = (int)chr - (int)'0';
		} else if(chr == '-') {
			positive = false;
		} else {
			throw new ProtocolException("expected a digit", currentPointer - 1);
		}

		while (currentPointer < limit) {
			chr = array[currentPointer++];
			if(chr == ' ') {
				break;
			}
			tmp *= 10;
			if (chr >= '0' && chr <= '9') {
				tmp += (int)chr - (int)'0';
			} else {
				throw new ProtocolException("expected a digit", currentPointer - 1);
			}
		}
		protocol.lineBuffer.position(currentPointer);
		return positive ? tmp : -tmp;
	}

	/**
	 * Returns the next token in the Protocol's lineBuffer as a String. The value is considered to end at the end of the
	 * lineBuffer or at a space. If no character is found, a ProtocolException is thrown.
	 *
	 * @param protocol An Old Mapi Protocol instance where the next token will be retrieved
	 * @return The next token in the Protocol as a String
	 * @throws ProtocolException if no character could be read
	 */
	static String getNextResponseDataAsString(OldMapiProtocol protocol) throws ProtocolException {
		int currentPointer = protocol.lineBuffer.position();
		int limit = protocol.lineBuffer.limit();
		char[] array = protocol.lineBuffer.array();

		if (currentPointer >= limit) {
			throw new ProtocolException("unexpected end of string", currentPointer - 1);
		}
		int cnt = 0, mark = currentPointer;
		char chr;

		while (currentPointer < limit) {
			chr = array[currentPointer++];
			if(chr == ' ') {
				break;
			}
			cnt++;
		}

		protocol.lineBuffer.position(mark);
		return new String(array, 0, cnt);
	}
}
