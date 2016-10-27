/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded.column;

import nl.cwi.monetdb.embedded.EmbeddedQueryResult;

import java.net.InetAddress;

/**
 * Mapping for MonetDB INET data type
 */
public class InetColumn extends Column<InetAddress> {

    private final InetAddress[] values;

    public InetColumn(EmbeddedQueryResult result, int index, InetAddress[] values, boolean[] nullIndex) {
        super(result, index, nullIndex);
        this.values = values;
    }

    @Override
    public InetAddress[] getAllValues() {
        return this.values;
    }

    @Override
    protected InetAddress getValueImplementation(int index) {
        return this.values[index];
    }
}
