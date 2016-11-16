/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded.resultset;

import nl.cwi.monetdb.embedded.mapping.AbstractResultTable;
import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedConnection;
import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedException;
import nl.cwi.monetdb.embedded.mapping.MonetDBRow;
import nl.cwi.monetdb.embedded.mapping.MonetDBToJavaMapping;

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
public class QueryResultSet extends AbstractResultTable implements Iterable {

    /**
     * The table C pointer.
     */
    protected long tablePointer;

    /**
     * The query result set columns listing.
     */
    private final QueryResultSetColumn<?>[] columns;

    /**
     * The number of rows in the query result.
     */
    protected final int numberOfRows;

	protected QueryResultSet(MonetDBEmbeddedConnection connection, long tablePointer, QueryResultSetColumn<?>[] columns,
                             int numberOfRows) {
        super(connection);
        this.tablePointer = tablePointer;
        this.numberOfRows = numberOfRows;
        this.columns = columns;
	}

    @Override
    public int getNumberOfRows() { return this.numberOfRows; }

    @Override
    public int getNumberOfColumns() { return this.columns.length; }

    @Override
    protected void closeImplementation() { this.cleanResultSet(this.tablePointer); }

    @Override
    public String[] getColumnNames() {
        int i = 0;
        String[] result = new String[this.getNumberOfColumns()];
        for(QueryResultSetColumn col : this.columns) {
            result[i] = col.getColumnName();
        }
        return result;
    }

    @Override
    public String[] getColumnTypes() {
        int i = 0;
        String[] result = new String[this.getNumberOfColumns()];
        for(QueryResultSetColumn col : this.columns) {
            result[i] = col.getColumnInternalTypeName();
        }
        return result;
    }

    @Override
    public MonetDBToJavaMapping[] getMappings() {
        int i = 0;
        MonetDBToJavaMapping[] result = new MonetDBToJavaMapping[this.getNumberOfColumns()];
        for(QueryResultSetColumn col : this.columns) {
            result[i] = col.getMapping();
        }
        return result;
    }

    @Override
    public int[] getColumnDigits() {
        int i = 0;
        int[] result = new int[this.getNumberOfColumns()];
        for(QueryResultSetColumn col : this.columns) {
            result[i] = col.getColumnDigits();
        }
        return result;
    }

    @Override
    public int[] getColumnScales() {
        int i = 0;
        int[] result = new int[this.getNumberOfColumns()];
        for(QueryResultSetColumn col : this.columns) {
            result[i] = col.getColumnScale();
        }
        return result;
    }

    /**
     * Tells if the connection of this statement result has been closed or not.
     *
     * @return A boolean indicating if the statement result has been cleaned or not
     */
    public boolean isStatementClosed() { return this.tablePointer == 0; }

    /**
     * Gets a column from the result set by index.
     *
     * @param index QueryResultSetColumn index (starting from 0)
     * @return The column
     */
    @SuppressWarnings("unchecked")
    public <T> QueryResultSetColumn<T> getColumnByIndex(int index) { return (QueryResultSetColumn<T>) columns[index]; }

    /**
     * Gets a column from the result set by name.
     *
     * @param name QueryResultSetColumn name
     * @return The column
     */
    @SuppressWarnings("unchecked")
    public <T> QueryResultSetColumn<T> getColumnByName(String name) {
        int index = 0;
        for (QueryResultSetColumn col : this.columns) {
            if (col.getColumnName().equals(name)) {
                return (QueryResultSetColumn<T>) this.columns[index];
            }
            index++;
        }
        throw new ArrayIndexOutOfBoundsException("The column is not present in the result set!");
    }

    /**
     * Fetches rows from the result set.
     *
     * @param startIndex The first row index to retrieve
     * @param endIndex The last row index to retrieve
     * @return The rows as {@code AbstractRowSet}
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
	public QueryResultRowSet fetchResultSetRows(int startIndex, int endIndex) throws MonetDBEmbeddedException {
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
        Object[][] temp = new Object[numberOfRowsToRetrieve][this.getNumberOfColumns()];
		for (int i = 0 ; i < this.getNumberOfColumns(); i++) {
            Object[] nextColumn = this.columns[i].fetchColumnValues(startIndex, endIndex);
            for(int j = 0; j < numberOfRowsToRetrieve ; j++) {
                temp[j][i] = nextColumn[j];
			}
		}
        return new QueryResultRowSet(this.getMappings(), temp, this);
	}

    /**
     * Fetches rows from the result set asynchronously.
     *
     * @param startIndex The first row index to retrieve
     * @param endIndex The last row index to retrieve
     * @return The rows as {@code AbstractRowSet}
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    /*public CompletableFuture<AbstractRowSet> fetchResultSetRowsAsync(int startIndex, int endIndex) throws MonetDBEmbeddedException {
        return CompletableFuture.supplyAsync(() -> this.fetchResultSetRows(startIndex, endIndex));
    }*/

    /**
     * Fetches the first N rows from the result set.
     *
     * @param n The last row index to retrieve
     * @return The rows as {@code AbstractRowSet}
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public QueryResultRowSet fetchFirstNRowValues(int n) throws MonetDBEmbeddedException {
        return this.fetchResultSetRows(0, n);
    }

    /**
     * Fetches the first N rows from the result set asynchronously.
     *
     * @param n The last row index to retrieve
     * @return The rows as {@code AbstractRowSet}
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    /*public CompletableFuture<AbstractRowSet> fetchFirstNRowValuesAsync(int n) throws MonetDBEmbeddedException {
        return this.fetchResultSetRowsAsync(0, n);
    }*/

    /**
     * Fetches all rows from the result set.
     *
     * @return The rows as {@code AbstractRowSet}
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public QueryResultRowSet fetchAllRowValues() throws MonetDBEmbeddedException {
        return this.fetchResultSetRows(0, this.numberOfRows);
    }

    /**
     * Fetches all rows from the result set asynchronously.
     *
     * @return The rows as {@code AbstractRowSet}
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    /*public CompletableFuture<AbstractRowSet> fetchAllRowValuesAsync() throws MonetDBEmbeddedException {
        return this.fetchResultSetRowsAsync(0, this.numberOfRows);
    }*/

    @Override
    public ListIterator<MonetDBRow> iterator() {
        try {
            return Arrays.asList(this.fetchAllRowValues().getAllRows()).listIterator();
        } catch (MonetDBEmbeddedException ex) {
            return null;
        }
    }

    private native void cleanResultSet(long tablePointer);
}
