/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.responses;

import nl.cwi.monetdb.mcl.protocol.AbstractProtocol;
import nl.cwi.monetdb.mcl.protocol.ProtocolException;

public interface IIncompleteResponse extends IResponse {
    /**
     * Returns whether this Response expects more lines to be added to it.
     *
     * @return true if a next line should be added, false otherwise
     */
    boolean wantsMore();

    void addLines(AbstractProtocol protocol) throws ProtocolException;
}
