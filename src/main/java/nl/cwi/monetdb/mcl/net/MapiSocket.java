/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.net;

/**
 * a wrapper class for old programs who still depend on
 * class nl.cwi.monetdb.mcl.net.MapiSocket to work.
 * This class is deprecated since nov 2020 and will be removed in a future release.
 */
public final class MapiSocket extends org.monetdb.mcl.net.MapiSocket {
	/**
	 * Constructs a new MapiSocket.
	 */
	public MapiSocket() {
		super();
	}
}
