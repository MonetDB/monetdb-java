/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded.column;

import nl.cwi.monetdb.jdbc.MonetClob;
import nl.cwi.monetdb.embedded.EmbeddedQueryResult;

/**
 * Mapping for MonetDB CLOB data type
 */
public class ClobColumn extends Column<String> {

    private final String[] values;

    public ClobColumn(EmbeddedQueryResult result, int index, String[] values, boolean[] nullIndex) {
        super(result, index, nullIndex);
        this.values = values;
    }

    @Override
    public String[] getAllValues() {
        return this.values;
    }

    @Override
    protected String getValueImplementation(int index) {
        return this.values[index];
    }
}
