/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.protocol.oldmapi;

import nl.cwi.monetdb.mcl.protocol.ServerResponses;

/**
 * This class parses the next server response on a MAPI connection using the next ASCII character on the stream.
 *
 * @author Fabian Groffen, Pedro Ferreira
 */
final class OldMapiServerResponseParser {

	private OldMapiServerResponseParser() {}

	/**
	 * Retrieves the next server response from an old MAPI protocol instance.
	 *
	 * @param protocol An Old MAPI protocol instance from which the next server response will be retrieved
	 * @return The integer representation of the next server response
	 */
	static int parseOldMapiServerResponse(OldMapiProtocol protocol) {
		int res;
		switch (protocol.lineBuffer.get()) {
			case '!':
				res = ServerResponses.ERROR;
				break;
			case '&':
				res = ServerResponses.SOHEADER;
				break;
			case '%':
				res = ServerResponses.HEADER;
				break;
			case '[':
				res = ServerResponses.RESULT;
				break;
			case '=':
				res = ServerResponses.RESULT;
				break;
			case '^':
				res = ServerResponses.REDIRECT;
				break;
			case '#':
				res = ServerResponses.INFO;
				break;
			case '.':
				res = ServerResponses.PROMPT;
				break;
			case ',':
				res = ServerResponses.MORE;
				break;
			default:
				res = ServerResponses.UNKNOWN;
		}
		return res;
	}
}
