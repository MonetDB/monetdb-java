/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded.column;

import nl.cwi.monetdb.embedded.types.MonetDBEmbeddedBlob;
import nl.cwi.monetdb.embedded.EmbeddedQueryResult;

/**
 * Mapping for MonetDB BLOB data type
 */
public class BlobColumn extends Column<MonetDBEmbeddedBlob> {

    private final MonetDBEmbeddedBlob[] values;

    public BlobColumn(EmbeddedQueryResult result, int index, MonetDBEmbeddedBlob[] values, boolean[] nullIndex) {
        super(result, index, nullIndex);
        this.values = values;
    }

    @Override
    public MonetDBEmbeddedBlob[] getAllValues() {
        return this.values;
    }

    @Override
    protected MonetDBEmbeddedBlob getValueImplementation(int index) {
        return this.values[index];
    }
}
