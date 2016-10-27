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
 * Mapping for MonetDB TINYINT data type
 */
public class TinyintColumn extends Column<Byte> {

	private final Byte[] values;

	public TinyintColumn(EmbeddedQueryResult result, int index, byte[] values, boolean[] nullIndex) {
		super(result, index, nullIndex);
		Byte[] newArray = new Byte[values.length];
		int j = newArray.length;
		for(int i = 0 ; i < j ; i++) {
			if (nullIndex[i]) {
				newArray[i] = null;
			} else {
				newArray[i] = Byte.valueOf(values[i]);
			}
		}
		this.values = newArray;
	}

	@Override
	public Byte[] getAllValues() {
		return this.values;
	}

	@Override
	protected Byte getValueImplementation(int index) {
		return this.values[index];
	}
}
