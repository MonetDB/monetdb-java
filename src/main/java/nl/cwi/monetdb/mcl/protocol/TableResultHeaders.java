/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.protocol;

/**
 * This enum lists the result table headers returned by the server.
 */
public enum TableResultHeaders {

    /* Please don't change the order */

    /** The column names */
    NAME(1),
    /** The column lengths */
    LENGTH(2),
    /** The column table and schemas names in format of schema.table */
    TABLE(4),
    /** The SQL name of the MonetDB data type of the column */
    TYPE(8),
    /** This header is returned by the JDBC embedded telling that it fetches all the previous headers at once */
    ALL(15),
    /** When an unknown table header is returned on a MAPI connection */
    UNKNOWN(0);

    /** An integer value for the bitmap on the ResultSetResponse Class */
    private final int valueForBitMap;

    TableResultHeaders(int valueForBitMap) {
        this.valueForBitMap = valueForBitMap;
    }

    /**
     * Returns the integer value for the bitmap.
     *
     * @return The integer value for the bitmap.
     */
    public int getValueForBitMap() {
        return valueForBitMap;
    }
}
