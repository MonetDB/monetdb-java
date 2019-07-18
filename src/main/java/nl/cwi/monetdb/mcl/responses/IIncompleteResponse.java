/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.responses;

import nl.cwi.monetdb.mcl.protocol.AbstractProtocol;
import nl.cwi.monetdb.mcl.protocol.ProtocolException;

/**
 * The ResultSetResponse and DatablockResponse Classes might require more than one Block response if the response is
 * larger than the BlockSize.
 *
 * @author Fabian Groffen, Pedro Ferreira
 */
public interface IIncompleteResponse extends IResponse {

	/**
	 * Returns whether this Response expects more lines to be added to it.
	 *
	 * @return true if a next line should be added, false otherwise
	 */
	boolean wantsMore();

	/**
	 * Adds a batch of data to the Response instance.
	 *
	 * @param protocol The connection's protocol to fetch data from
	 * @throws ProtocolException If the result line is not expected
	 */
	void addLines(AbstractProtocol protocol) throws ProtocolException;
}
