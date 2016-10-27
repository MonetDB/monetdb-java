/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded.column;

import nl.cwi.monetdb.embedded.EmbeddedQueryResult;

import java.sql.Date;

/**
 * Mapping for MonetDB DATE data type
 */
public class DateColumn extends Column<Date> {

    private final Date[] values;

    public DateColumn(EmbeddedQueryResult result, int index, Date[] values, boolean[] nullIndex) {
        super(result, index, nullIndex);
        this.values = values;
    }

    @Override
    public Date[] getAllValues() {
        return this.values;
    }

    @Override
    protected Date getValueImplementation(int index) {
        return this.values[index];
    }
}
