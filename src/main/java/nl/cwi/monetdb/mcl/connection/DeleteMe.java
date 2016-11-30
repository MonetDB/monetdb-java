/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.connection;

import java.io.IOException;


public final class DeleteMe extends MapiConnection {


	public DeleteMe(String database, boolean debug, MonetDBLanguage lang, String hostname, int port) throws IOException {
		super(database, debug, lang, hostname, port, 9);
	}

}
