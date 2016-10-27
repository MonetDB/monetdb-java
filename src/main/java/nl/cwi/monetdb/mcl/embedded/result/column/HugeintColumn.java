/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.embedded.result.column;

import nl.cwi.monetdb.mcl.embedded.result.EmbeddedQueryResult;

import java.math.BigInteger;

/**
 * Mapping for MonetDB HUGEINT data type
 */
public class HugeintColumn extends Column<BigInteger> {

    private final BigInteger[] values;

    public HugeintColumn(EmbeddedQueryResult result, int index, BigInteger[] values, boolean[] nullIndex) {
        super(result, index, nullIndex);
        this.values = values;
    }

    @Override
    public BigInteger[] getAllValues() {
        return this.values;
    }

    @Override
    protected BigInteger getValueImplementation(int index) {
        return this.values[index];
    }
}
