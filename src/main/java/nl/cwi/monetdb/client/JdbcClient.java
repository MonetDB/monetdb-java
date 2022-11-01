/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

package nl.cwi.monetdb.client;

import java.sql.DriverManager;	// import is required as it will load the org.monetdb.jdbc.MonetDriver class

/**
 * a wrapper class for old programs who still depend on
 * class nl.cwi.monetdb.client.JdbcClient to work.
 * This class is deprecated since nov 2020 and will be removed in a future release.
 */
public final class JdbcClient extends org.monetdb.client.JdbcClient {
}
