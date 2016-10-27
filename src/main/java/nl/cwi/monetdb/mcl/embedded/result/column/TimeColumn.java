/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.embedded.result.column;

import nl.cwi.monetdb.mcl.embedded.result.EmbeddedQueryResult;

import java.sql.Time;

/**
 * Mapping for MonetDB TIME data type
 */
public class TimeColumn extends Column<Time> {

    private final Time[] values;

    public TimeColumn(EmbeddedQueryResult result, int index, Time[] values, boolean[] nullIndex) {
        super(result, index, nullIndex);
        this.values = values;
    }

    @Override
    public Time[] getAllValues() {
        return this.values;
    }

    @Override
    protected Time getValueImplementation(int index) {
        return this.values[index];
    }
}
