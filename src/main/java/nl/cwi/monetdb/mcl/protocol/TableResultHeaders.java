/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.protocol;

/**
 * This class lists the result table headers returned by the server. The integer values are used for the bitmap on the
 * ResultSetResponse Class.
 *
 * @author Fabian Groffen, Pedro Ferreira
 */
public final class TableResultHeaders {

    /* Please don't change the order */

    /** When an unknown table header is returned on a MAPI connection */
    public static final int UNKNOWN = 0;
    /** The column names */
    public static final int NAME = 1;
    /** The column lengths */
    public static final int LENGTH = 2;
    /** The column table and schemas names in format of schema.table */
    public static final int TABLE = 4;
    /** The SQL name of the MonetDB data type of the column */
    public static final int TYPE = 8;
    /** This header is returned by the JDBC embedded telling that it fetches all the previous headers at once */
    public static final int ALL = 15;
}
