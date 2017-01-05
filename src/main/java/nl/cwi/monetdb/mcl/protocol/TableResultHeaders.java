/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.protocol;

public enum TableResultHeaders {

    /* Please don't change the order */
    NAME(1),
    LENGTH(2),
    TABLE(4),
    TYPE(8),
    ALL(15),
    UNKNOWN(0);

    private final int valueForBitMap;

    TableResultHeaders(int valueForBitMap) {
        this.valueForBitMap = valueForBitMap;
    }

    public int getValueForBitMap() {
        return valueForBitMap;
    }
}
