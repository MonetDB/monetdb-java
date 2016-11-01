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
public class QueryResultSet extends AbstractQueryResultSet {

	/**
	 * The query result set columns listing
	 */
    private final QueryResultSetColumn<?>[] columns;

	protected QueryResultSet(MonetDBEmbeddedConnection connection, long resultPointer,
                             QueryResultSetColumn<?>[] columns, int numberOfRows) {
        super(connection, resultPointer, columns.length, numberOfRows);
        this.columns = columns;
		this.resultPointer = resultPointer;
	}

    /**
     * Get the query set column values as an Iterable.
     *
     * @return An Iterable over the columns
     */
    @Override
    protected Iterable<AbstractColumn<?>> getIterable() {
        return Arrays.asList(columns);
    }

    /**
     * Get a columns' values from the result set by index.
     *
     * @param index QueryResultSetColumn index (starting from 0)
     * @return The columns, {@code null} if index not in bounds
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> QueryResultSetColumn<T> getColumn(int index) {
        return (QueryResultSetColumn<T>) columns[index];
    }

    /**
     * Fetches rows from the result set.
     *
     * @param startIndex The first row index to retrieve
     * @param endIndex The last row index to retrieve
     * @return The rows as {@code QueryRowsResultSet}
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
	public QueryRowsResultSet fetchResultSetRows(int startIndex, int endIndex) throws MonetDBEmbeddedException {
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
        return new QueryRowsResultSet(this, this.getMappings(), temp);
	}

    /**
     * Fetches rows from the result set asynchronously.
     *
     * @param startIndex The first row index to retrieve
     * @param endIndex The last row index to retrieve
     * @return The rows as {@code QueryRowsResultSet}
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public QueryRowsResultSet fetchResultSetRowsAsync(int startIndex, int endIndex) throws MonetDBEmbeddedException {
        /* CompletableFuture.supplyAsync(() -> this.fetchResultSetRows(startIndex, endIndex)); */
        throw new UnsupportedOperationException("Must wait for Java 8 :(");
    }

    /**
     * Fetches the first N rows from the result set.
     *
     * @param n The last row index to retrieve
     * @return The rows as {@code QueryRowsResultSet}
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public QueryRowsResultSet fetchFirstNRowValues(int n) throws MonetDBEmbeddedException {
        return this.fetchResultSetRows(0, n);
    }

    /**
     * Fetches the first N rows from the result set asynchronously.
     *
     * @param n The last row index to retrieve
     * @return The rows as {@code QueryRowsResultSet}
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public QueryRowsResultSet fetchFirstNRowValuesAsync(int n) throws MonetDBEmbeddedException {
        return this.fetchResultSetRowsAsync(0, n);
    }

    /**
     * Fetches all rows from the result set.
     *
     * @return The rows as {@code QueryRowsResultSet}
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public QueryRowsResultSet fetchAllRowValues() throws MonetDBEmbeddedException {
        return this.fetchResultSetRows(0, this.numberOfRows);
    }

    /**
     * Fetches all rows from the result set asynchronously.
     *
     * @return The rows as {@code QueryRowsResultSet}
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public QueryRowsResultSet fetchAllRowValuesAsync() throws MonetDBEmbeddedException {
        return this.fetchResultSetRowsAsync(0, this.numberOfRows);
    }

    @Override
    public ListIterator<QueryRowsResultSet.QueryResulSetSingleRow> iterator() {
        try {
            return Arrays.asList(this.fetchAllRowValues().getAllRows()).listIterator();
        } catch (MonetDBEmbeddedException ex) {
            return null;
        }
    }
}
