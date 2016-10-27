/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded.column;

import nl.cwi.monetdb.embedded.EmbeddedQueryResult;

/**
 * Mapping for MonetDB BIGINT data type
 */
public class BigintColumn extends Column<Long> {

	private final Long[] values;

	public BigintColumn(EmbeddedQueryResult result, int index, long[] values, boolean[] nullIndex) {
		super(result, index, nullIndex);
		Long[] newArray = new Long[values.length];
		int j = newArray.length;
		for(int i = 0 ; i < j ; i++) {
			if (nullIndex[i]) {
				newArray[i] = null;
			} else {
				newArray[i] = Long.valueOf(values[i]);
			}
		}
		this.values = newArray;
	}

	@Override
	public Long[] getAllValues() {
		return this.values;
	}

	@Override
	protected Long getValueImplementation(int index) {
		return this.values[index];
	}
}
