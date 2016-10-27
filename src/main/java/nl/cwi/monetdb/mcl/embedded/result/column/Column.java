/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.embedded.result.column;

import nl.cwi.monetdb.mcl.embedded.result.EmbeddedQueryResult;

import java.util.Iterator;

/**
 *  Am abstract class for accessing, 
 *  materialised (Java-level) query result columns.
 *
 * @param <T> A primitive or String type
 */
public abstract class Column<T> implements Iterable<T> {

	/**
	 * The name of the column in the query result
	 */
	protected final String columnName;

	/**
	 * The type of the column in the query result
	 */
	protected final String columnType;

	/**
	 * The size/length of the column.
	 */
	protected final int columnSize;

    /**
     * Array with null values;
     */
    protected final boolean[] nullIndex;

	public Column(EmbeddedQueryResult result, int index, boolean[] nullIndex) {
		this.columnName = result.getColumnNames()[index];
		this.columnType = result.getColumnTypes()[index];
		this.columnSize = result.getColumnSizes()[index];
        this.nullIndex = nullIndex;
 	}

	/**
	 * Get the name of the column.
	 *
	 * @return Column name
	 */
	public String getColumnName() {
		return columnName;
	}

	/**
	 * Get the type of the column.
	 *
	 * @return Column type
	 */
	public String getColumnType() { return columnType; }

	/**
	 * Get the size of the column.
	 *
	 * @return Column size
	 */
	public int getColumnSize() { return columnSize; }

    /**
     * Get the array mapping of null values
     *
     * @return Null values
     */
    public boolean[] getNullindex() { return nullIndex; }

    /**
	 * Get a (non-primary-type) value at index of a column.
	 *
	 * @param index Column index for the value
	 * @return Value, cloud be {@code null}
	 */
	public T getValue(int index) {
		if (index < 0) {
			throw new ArrayIndexOutOfBoundsException("The index must be larger than 0!");
		} else if (index >= this.columnSize) {
			throw new ArrayIndexOutOfBoundsException("The index must be smaller than the number of elements in the column!");
		}
		return this.getValueImplementation(index);
	}

	/**
	 * Get all values of the column
	 *
	 * @return All values of the column
	 */
	public abstract T[] getAllValues();

	protected abstract T getValueImplementation(int index);

	@Override
	public Iterator<T> iterator() {
		return new Iterator<T>() {
			private int currentIndex = 0;

			@Override
			public boolean hasNext() {
				return (currentIndex < getColumnSize());
			}

			@Override
			public T next() {
				return getValue(currentIndex++);
			}
		};
	}
}
