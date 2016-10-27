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
 * Mapping for MonetDB BOOLEAN data type
 */
public class BooleanColumn extends Column<Boolean> {

	private final Boolean[] values;

	public BooleanColumn(EmbeddedQueryResult result, int index, boolean[] values, boolean[] nullIndex) {
		super(result, index, nullIndex);
		Boolean[] newArray = new Boolean[values.length];
		int j = newArray.length;
		for(int i = 0 ; i < j ; i++) {
			if (nullIndex[i]) {
				newArray[i] = null;
			} else {
				newArray[i] = Boolean.valueOf(values[i]);
			}
		}
		this.values = newArray;
	}

	@Override
	public Boolean[] getAllValues() {
		return this.values;
	}

	@Override
	protected Boolean getValueImplementation(int index) {
		return this.values[index];
	}
}
