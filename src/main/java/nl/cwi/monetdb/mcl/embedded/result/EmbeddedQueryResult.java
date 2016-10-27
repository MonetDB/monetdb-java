/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.embedded.result;

import java.io.Closeable;
import java.sql.SQLException;
import java.util.Iterator;

import nl.cwi.monetdb.mcl.embedded.result.column.Column;

/**
 * Embedded MonetDB query result.
 * The query result columns are not eagerly copied from the native code to Java.
 * Instead, they are kept around at MonetDB native C-level, materialised in Java 
 * on demand and freed on {@code super.close()}.
 *
 */
public class EmbeddedQueryResult implements Closeable, Iterable<Column<?>> {
	/**
	 * The names of the columns in the query result.
	 */
	protected final String[] columnNames;
	/**
	 * The types of the columns in the query result.
	 */
    protected final String[] columnTypes;
    /**
     * The sizes of the columns in the query result.
     */
    protected final int[] columnSizes;
	/**
	 * The number of columns in the query result.
	 */
    protected final int numberOfColumns;
	/**
	 * The number of rows in the query result.
	 */
    protected final int numberOfRows;
	/**
	 * Pointer to the native result set.
	 * We need to keep it around for getting columns.
	 * The native result set is kept until the {@link super.close()} is called.
	 */
    protected long resultPointer;
	/**
	 * To avoid reconstructing the columns a second time, we will use this cache
	 */
    protected final Column<?>[] columnsCache;

	public EmbeddedQueryResult(String[] columnNames, String[] columnTypes, int[] columnSizes, int numberOfColumns, int numberOfRows, long resultPointer) {
		this.columnNames = columnNames;
		this.columnTypes = columnTypes;
        this.columnSizes = columnSizes;
		this.numberOfColumns = numberOfColumns;
		this.numberOfRows = numberOfRows;
		this.resultPointer = resultPointer;
		this.columnsCache = new Column<?>[numberOfColumns];
	}

	/**
	 * Get the column names as a string array.
	 * 
	 * @return The column names array
	 */
	public String[] getColumnNames() {
		return columnNames;
	}

	/**
	 * Get the column types as a string array.
	 * 
	 * @return The column types array
	 */
	public String[] getColumnTypes() {
		return columnTypes;
	}

    /**
     * Get the column sizes as a int array.
     *
     * @return The column sizes array
     */
    public int[] getColumnSizes() {
        return columnSizes;
    }

	/**
	 * Returns the number of columns in the result set.
	 *
	 * @return Number of columns
	 */
	public int getNumberOfColumns() {
		return numberOfColumns;
	}

	/**
	 * Returns the number of rows in the result set.
	 *
	 * @return Number of rows
	 */
	public int getNumberOfRows() {
		return numberOfRows;
	}

	/**
	 * Get a column from the result set by index.
	 *
	 * @param index Column index (starting from 0)
	 * @return The column, {@code null} if index not in bounds
	 */
	public Column<?> getColumn(int index) throws SQLException {
		if (index < 0) {
			throw new ArrayIndexOutOfBoundsException("The index must be larger than 0!");
		} else if (index >= this.numberOfColumns) {
			throw new ArrayIndexOutOfBoundsException("The index must be smaller than the number of columns");
		}
		if(this.columnsCache[index] != null) {
			return this.columnsCache[index];
		}
		if (this.resultPointer == 0) {
			// The object was closed and result was cleaned-up. Calling the can produce a native Segfault (and crash the JVM)
			throw new NullPointerException("The result set has been already cleaned!");
		}
		Column<?> result = this.getColumnWrapper(index, this.resultPointer);
		this.columnsCache[index] = result;
		return result;
	}

	/**
	 * Get a column from the result set by name.
	 *
	 * @param name Column name
	 * @return The column, {@code null} if not found
	 */
	public Column<?> getColumn(String name) throws SQLException {
		int index = 0;
		for (String columnName : this.columnNames) {
			if (name.equals(columnName)) {
				return this.getColumn(index);
			}
			index++;
		}
		throw new ArrayIndexOutOfBoundsException("The column is not present in the result set!");
	}

	/**
	 * A native C function that returns a {@code Column} object.
	 *
	 * @param index Column index (starting from 0) 
	 * @return
	 */
	private native Column<?> getColumnWrapper(int index, long resultPointer) throws SQLException;

	@Override
	public Iterator<Column<?>> iterator() {
		return new Iterator<Column<?>>() {
			private int currentIndex = 0;

			@Override
			public boolean hasNext() {
				return (currentIndex < getNumberOfColumns());
			}

			@Override
			public Column<?> next() {
				try {
					return getColumn(currentIndex++);
				} catch (SQLException ex) {
					return null;
				}
			}
		};
	}

	/**
	 * Get a matrix of objects representing the rows and columns of the query
	 *
	 * @return The rows as {@code  Object[][]}
	 */
	public Object[][] getRows() throws SQLException {
		Object[][] result = new Object[this.numberOfRows][this.numberOfColumns];
		Column<?> column;
		for (int i = 0 ; i < this.numberOfColumns; i++) {
			column = this.getColumn(i);
			for (int j = 0 ; j < this.numberOfRows; j++) {
				result[j][i] = column.getValue(i);
			}
		}
		return result;
	}

	@Override
	public void close() {
		if(this.resultPointer > 0) {
			this.cleanupResult(this.resultPointer);
			this.resultPointer = 0;
		}
	}

	/** 
	 * Free the C-level result structure.
	 */
	private native void cleanupResult(long resultPointer);
}
