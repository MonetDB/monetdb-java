/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.embedded.result.column;

import nl.cwi.monetdb.mcl.embedded.result.EmbeddedQueryResult;

/**
 * Mapping for MonetDB SMALLINT data type
 */
public class SmallintColumn extends Column<Short> {

	private final Short[] values;

	public SmallintColumn(EmbeddedQueryResult result, int index, short[] values, boolean[] nullIndex) {
		super(result, index, nullIndex);
		Short[] newArray = new Short[values.length];
		int j = newArray.length;
		for(int i = 0 ; i < j ; i++) {
			if (nullIndex[i]) {
				newArray[i] = null;
			} else {
				newArray[i] = Short.valueOf(values[i]);
			}
		}
		this.values = newArray;
	}

	@Override
	public Short[] getAllValues() {
		return this.values;
	}

	@Override
	protected Short getValueImplementation(int index) {
		return this.values[index];
	}
}
