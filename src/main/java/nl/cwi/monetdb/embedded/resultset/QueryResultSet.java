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
    private final AbstractQueryResultSetColumn<?>[] columns;

    /**
     * The number of rows in the query result.
     */
    protected final int numberOfRows;

	protected QueryResultSet(MonetDBEmbeddedConnection connection, long tablePointer,
                             AbstractQueryResultSetColumn<?>[] columns, int numberOfRows) {
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
        for(AbstractQueryResultSetColumn col : this.columns) {
            result[i] = col.getColumnName();
        }
        return result;
    }

    @Override
    public String[] getColumnTypes() {
        int i = 0;
        String[] result = new String[this.getNumberOfColumns()];
        for(AbstractQueryResultSetColumn col : this.columns) {
            result[i] = col.getColumnInternalTypeName();
        }
        return result;
    }

    @Override
    public MonetDBToJavaMapping[] getMappings() {
        int i = 0;
        MonetDBToJavaMapping[] result = new MonetDBToJavaMapping[this.getNumberOfColumns()];
        for(AbstractQueryResultSetColumn col : this.columns) {
            result[i] = col.getMapping();
        }
        return result;
    }

    @Override
    public int[] getColumnDigits() {
        int i = 0;
        int[] result = new int[this.getNumberOfColumns()];
        for(AbstractQueryResultSetColumn col : this.columns) {
            result[i] = col.getColumnDigits();
        }
        return result;
    }

    @Override
    public int[] getColumnScales() {
        int i = 0;
        int[] result = new int[this.getNumberOfColumns()];
        for(AbstractQueryResultSetColumn col : this.columns) {
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
     * Gets a column index from the result set by name
     *
     * @param columnName AbstractQueryResultSetColumn name
     * @return The index number
     */
    public int getColumnIndexByName(String columnName) {
        int index = 0;
        for (AbstractQueryResultSetColumn col : this.columns) {
            if (col.getColumnName().equals(columnName)) {
                return index;
            }
            index++;
        }
        throw new ArrayIndexOutOfBoundsException("The column is not present in the result set!");
    }

    /**
     * Gets a column from the result set by index.
     *
     * @param index AbstractQueryResultSetColumn index (starting from 0)
     * @return The column
     */
    protected AbstractQueryResultSetColumn<?> getColumnByIndex(int index) {
        return this.columns[index];
    }

    /**
     * Gets a column from the result set by name.
     *
     * @param columnName AbstractQueryResultSetColumn name
     * @return The column
     */
    protected AbstractQueryResultSetColumn<?> getColumnByName(String columnName) {
        int index = this.getColumnIndexByName(columnName);
        return this.columns[index];
    }

    /**
     * Gets a boolean column from the result set by index.
     *
     * @param index QueryResultSetBooleanColumn index (starting from 0)
     * @return The boolean column
     */
    public QueryResultSetBooleanColumn getBooleanColumnByIndex(int index) {
        return (QueryResultSetBooleanColumn) this.columns[index];
    }

    /**
     * Gets a boolean column from the result set by name.
     *
     * @param columnName QueryResultSetBooleanColumn name
     * @return The boolean column
     */
    public QueryResultSetBooleanColumn getBooleanColumnByName(String columnName) {
        int index = this.getColumnIndexByName(columnName);
        return (QueryResultSetBooleanColumn) this.columns[index];
    }

    /**
     * Gets a byte column from the result set by index.
     *
     * @param index QueryResultSetByteColumn index (starting from 0)
     * @return The byte column
     */
    public QueryResultSetByteColumn getByteColumnByIndex(int index) {
        return (QueryResultSetByteColumn) this.columns[index];
    }

    /**
     * Gets a byte column from the result set by name.
     *
     * @param columnName QueryResultSetByteColumn name
     * @return The byte column
     */
    public QueryResultSetByteColumn getByteColumnByName(String columnName) {
        int index = this.getColumnIndexByName(columnName);
        return (QueryResultSetByteColumn) this.columns[index];
    }

    /**
     * Gets a short column from the result set by index.
     *
     * @param index QueryResultSetShortColumn index (starting from 0)
     * @return The short column
     */
    public QueryResultSetShortColumn getShortColumnByIndex(int index) {
        return (QueryResultSetShortColumn) this.columns[index];
    }

    /**
     * Gets a short column from the result set by name.
     *
     * @param columnName QueryResultSetShortColumn name
     * @return The short column
     */
    public QueryResultSetShortColumn getShortColumnByName(String columnName) {
        int index = this.getColumnIndexByName(columnName);
        return (QueryResultSetShortColumn) this.columns[index];
    }

    /**
     * Gets a int column from the result set by index.
     *
     * @param index QueryResultSetIntColumn index (starting from 0)
     * @return The int column
     */
    public QueryResultSetIntColumn getIntColumnByIndex(int index) {
        return (QueryResultSetIntColumn) this.columns[index];
    }

    /**
     * Gets a int column from the result set by name.
     *
     * @param columnName QueryResultSetIntColumn name
     * @return The int column
     */
    public QueryResultSetIntColumn getIntColumnByName(String columnName) {
        int index = this.getColumnIndexByName(columnName);
        return (QueryResultSetIntColumn) this.columns[index];
    }

    /**
     * Gets a long column from the result set by index.
     *
     * @param index QueryResultSetLongColumn index (starting from 0)
     * @return The long column
     */
    public QueryResultSetLongColumn getLongColumnByIndex(int index) {
        return (QueryResultSetLongColumn) this.columns[index];
    }

    /**
     * Gets a long column from the result set by name.
     *
     * @param columnName QueryResultSetLongColumn name
     * @return The long column
     */
    public QueryResultSetLongColumn getLongColumnByName(String columnName) {
        int index = this.getColumnIndexByName(columnName);
        return (QueryResultSetLongColumn) this.columns[index];
    }

    /**
     * Gets a float column from the result set by index.
     *
     * @param index QueryResultSetFloatColumn index (starting from 0)
     * @return The float column
     */
    public QueryResultSetFloatColumn getFloatColumnByIndex(int index) {
        return (QueryResultSetFloatColumn) this.columns[index];
    }

    /**
     * Gets a float column from the result set by name.
     *
     * @param columnName QueryResultSetFloatColumn name
     * @return The float column
     */
    public QueryResultSetFloatColumn getFloatColumnByName(String columnName) {
        int index = this.getColumnIndexByName(columnName);
        return (QueryResultSetFloatColumn) this.columns[index];
    }

    /**
     * Gets a double column from the result set by index.
     *
     * @param index QueryResultSetDoubleColumn index (starting from 0)
     * @return The double column
     */
    public QueryResultSetDoubleColumn getDoubleColumnByIndex(int index) {
        return (QueryResultSetDoubleColumn) this.columns[index];
    }

    /**
     * Gets a double column from the result set by name.
     *
     * @param columnName QueryResultSetDoubleColumn name
     * @return The double column
     */
    public QueryResultSetDoubleColumn getDoubleColumnByName(String columnName) {
        int index = this.getColumnIndexByName(columnName);
        return (QueryResultSetDoubleColumn) this.columns[index];
    }

    /**
     * Gets an object column from the result set by index.
     *
     * @param <T> The Java class of the mapped MonetDB column
     * @param index QueryResultSetObjectColumn index (starting from 0)
     * @return The object column
     */
    @SuppressWarnings("unchecked")
    public <T> QueryResultSetObjectColumn<T> getObjectColumnByIndex(int index) {
        return (QueryResultSetObjectColumn<T>) this.columns[index];
    }

    /**
     * Gets an object column from the result set by name.
     *
     * @param <T> The Java class of the mapped MonetDB column
     * @param columnName QueryResultSetObjectColumn name
     * @return The object column
     */
    @SuppressWarnings("unchecked")
    public <T> QueryResultSetObjectColumn<T> getObjectColumnByName(String columnName) {
        int index = this.getColumnIndexByName(columnName);
        return (QueryResultSetObjectColumn<T>) this.columns[index];
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
            Object[] nextColumn = this.columns[i].mapValuesToObjectArray(startIndex, endIndex);
            for(int j = 0; j < numberOfRowsToRetrieve; j++) {
                temp[j][i] = nextColumn[j];
			}
		}
        return new QueryResultRowSet(this, this.getMappings(), temp);
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
