/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.responses;

/**
 * A Response is a message sent by the server to indicate some action has taken place, and possible results of that
 * action.
 *
 * @author Fabian Groffen, Pedro Ferreira
 */
public interface IResponse {

    /**
     * Instructs the Response implementation to close and do the necessary clean up procedures.
     */
    void close();
}
