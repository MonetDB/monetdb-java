/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded.column;

import nl.cwi.monetdb.embedded.EmbeddedQueryResult;

import java.math.BigDecimal;

/**
 * Mapping for MonetDB DECIMAL data type
 */
public class DecimalColumn extends Column<BigDecimal> {

    private final BigDecimal[] values;

    private final int precision;

    private final int scale;

    public DecimalColumn(EmbeddedQueryResult result, int index, BigDecimal[] values, boolean[] nullIndex, int precision, int scale) {
        super(result, index, nullIndex);
        this.values = values;
        this.precision = precision;
        this.scale = scale;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    @Override
    public BigDecimal[] getAllValues() {
        return this.values;
    }

    @Override
    protected BigDecimal getValueImplementation(int index) {
        return this.values[index];
    }
}
