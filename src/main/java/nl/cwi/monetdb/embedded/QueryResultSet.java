/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded;

import java.util.Arrays;
import java.util.ListIterator;

/**
 * Embedded MonetDB query result.
 * The query result columns are not eagerly copied from the native code to Java.
 * Instead, they are kept around at MonetDB native C-level, materialised in Java 
 * on demand and freed on {@code super.close()}.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class QueryResultSet extends AbstractStatementResult implements Iterable {

    /**
     * Pointer to the native result set.
     * We need to keep it around for getting columns.
     * The native result set is kept until the the close method is called.
     */
    protected long resultPointer;

    /**
     * The number of columns in the query result.
     */
    protected final int numberOfColumns;

    /**
     * The number of rows in the query result.
     */
    protected final int numberOfRows;

	/**
	 * The query result set columns listing.
	 */
    private final QueryResultSetColumn<?>[] columns;

	protected QueryResultSet(MonetDBEmbeddedConnection connection, long resultPointer,
                             QueryResultSetColumn<?>[] columns, int numberOfRows) {
        super(connection);
        this.resultPointer = resultPointer;
        this.numberOfColumns = columns.length;
        this.numberOfRows = numberOfRows;
        this.columns = columns;
	}

    /**
     * Tells if the connection of this statement result has been closed or not.
     *
     * @return A boolean indicating if the statement result has been cleaned or not
     */
    public boolean isStatementClosed() { return this.resultPointer == 0; }

    /**
     * Returns the number of columns in the result set.
     *
     * @return Number of columns
     */
    public int getNumberOfColumns() {
        return this.numberOfColumns;
    }

    /**
     * Returns the number of rows in the result set.
     *
     * @return Number of rows
     */
    public int getNumberOfRows() {
        return this.numberOfRows;
    }

    /**
     * Get the columns names as a string array.
     *
     * @return The columns names array
     */
    public String[] getColumnNames() {
        int i = 0;
        String[] result = new String[this.numberOfColumns];
        for(AbstractColumn col : this.columns) {
            result[i] = col.getColumnName();
        }
        return result;
    }

    /**
     * Get the columns types as a string array.
     *
     * @return The columns types array
     */
    public String[] getColumnTypes() {
        int i = 0;
        String[] result = new String[this.numberOfColumns];
        for(AbstractColumn col : this.columns) {
            result[i] = col.getColumnType();
        }
        return result;
    }

    /**
     * Get the Java mappings as a MonetDBToJavaMapping array.
     *
     * @return The columns MonetDBToJavaMapping array
     */
    public MonetDBToJavaMapping[] getMappings() {
        int i = 0;
        MonetDBToJavaMapping[] result = new MonetDBToJavaMapping[this.numberOfColumns];
        for(AbstractColumn col : this.columns) {
            result[i] = col.getMapping();
        }
        return result;
    }

    /**
     * Get the columns digits as a int array.
     *
     * @return The columns digits array
     */
    public int[] getColumnDigits() {
        int i = 0;
        int[] result = new int[this.numberOfColumns];
        for(AbstractColumn col : this.columns) {
            result[i] = col.getColumnDigits();
        }
        return result;
    }

    /**
     * Get the columns scales as a int array.
     *
     * @return The columns scales array
     */
    public int[] getColumnScales() {
        int i = 0;
        int[] result = new int[this.numberOfColumns];
        for(AbstractColumn col : this.columns) {
            result[i] = col.getColumnScale();
        }
        return result;
    }

    /**
     * Get a columns' values from the result set by index.
     *
     * @param index QueryResultSetColumn index (starting from 0)
     * @return The columns, {@code null} if index not in bounds
     */
    @SuppressWarnings("unchecked")
    public <T> QueryResultSetColumn<T> getColumn(int index) {
        return (QueryResultSetColumn<T>) columns[index];
    }

    /**
     * Get a columns from the result set by name.
     *
     * @param name QueryResultSetColumn name
     * @return The columns
     */
    public <T> QueryResultSetColumn<T> getColumn(String name) {
        int index = 0;
        for (AbstractColumn col : this.columns) {
            if (col.getColumnName().equals(name)) {
                return this.getColumn(index);
            }
            index++;
        }
        throw new ArrayIndexOutOfBoundsException("The columns is not present in the result set!");
    }

    /**
     * Fetches rows from the result set.
     *
     * @param startIndex The first row index to retrieve
     * @param endIndex The last row index to retrieve
     * @return The rows as {@code QueryResultSetRows}
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
	public QueryResultSetRows fetchResultSetRows(int startIndex, int endIndex) throws MonetDBEmbeddedException {
        if(endIndex < startIndex) {
            int aux = startIndex;
            startIndex = endIndex;
            endIndex = aux;
        }
        if (startIndex < 0) {
            throw new ArrayIndexOutOfBoundsException("The start index must be larger than 0!");
        } else if (endIndex > this.numberOfRows) {
            throw new ArrayIndexOutOfBoundsException("The index must be smaller than the number of elements in the columns!");
        } else if(startIndex == endIndex) {
            throw new ArrayIndexOutOfBoundsException("Retrieving 0 rows?");
        }
        int numberOfRowsToRetrieve = endIndex - startIndex;
        Object[][] temp = new Object[numberOfRowsToRetrieve][this.numberOfColumns];
		for (int i = 0 ; i < this.numberOfColumns; i++) {
            Object[] nextColumn = this.columns[i].fetchColumnValues(startIndex, endIndex);
            for(int j = 0; j < numberOfRowsToRetrieve ; j++) {
                temp[j][i] = nextColumn[j];
			}
		}
        return new QueryResultSetRows(this, this.getMappings(), temp);
	}

    /**
     * Fetches rows from the result set asynchronously.
     *
     * @param startIndex The first row index to retrieve
     * @param endIndex The last row index to retrieve
     * @return The rows as {@code QueryResultSetRows}
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public QueryResultSetRows fetchResultSetRowsAsync(int startIndex, int endIndex) throws MonetDBEmbeddedException {
        /* CompletableFuture.supplyAsync(() -> this.fetchResultSetRows(startIndex, endIndex)); */
        throw new UnsupportedOperationException("Must wait for Java 8 :(");
    }

    /**
     * Fetches the first N rows from the result set.
     *
     * @param n The last row index to retrieve
     * @return The rows as {@code QueryResultSetRows}
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public QueryResultSetRows fetchFirstNRowValues(int n) throws MonetDBEmbeddedException {
        return this.fetchResultSetRows(0, n);
    }

    /**
     * Fetches the first N rows from the result set asynchronously.
     *
     * @param n The last row index to retrieve
     * @return The rows as {@code QueryResultSetRows}
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public QueryResultSetRows fetchFirstNRowValuesAsync(int n) throws MonetDBEmbeddedException {
        return this.fetchResultSetRowsAsync(0, n);
    }

    /**
     * Fetches all rows from the result set.
     *
     * @return The rows as {@code QueryResultSetRows}
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public QueryResultSetRows fetchAllRowValues() throws MonetDBEmbeddedException {
        return this.fetchResultSetRows(0, this.numberOfRows);
    }

    /**
     * Fetches all rows from the result set asynchronously.
     *
     * @return The rows as {@code QueryResultSetRows}
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public QueryResultSetRows fetchAllRowValuesAsync() throws MonetDBEmbeddedException {
        return this.fetchResultSetRowsAsync(0, this.numberOfRows);
    }

    @Override
    public ListIterator<QueryResultSetRows.QueryResulSetRow> iterator() {
        try {
            return Arrays.asList(this.fetchAllRowValues().getAllRows()).listIterator();
        } catch (MonetDBEmbeddedException ex) {
            return null;
        }
    }

    /**
     * Close the query data so no more new results can be retrieved.
     */
    @Override
    public void close() {
        this.cleanupResultInternal(this.resultPointer);
        this.resultPointer = 0;
        super.close();
    }

    private native void cleanupResultInternal(long resultPointer);
}
