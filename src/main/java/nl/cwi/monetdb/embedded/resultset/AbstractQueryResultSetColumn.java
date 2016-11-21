/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded.resultset;

import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedException;
import nl.cwi.monetdb.embedded.mapping.AbstractColumn;

/**
 * An abstract class for accessing materialised (Java-level) query result columns.
 *
 * @param <A> An array of a Java primitive mapped from a MonetDB column !! Must be a Java array !!
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public abstract class AbstractQueryResultSetColumn<A> extends AbstractColumn {

    /**
     * Internal C pointer of the column,
     */
    protected long tablePointer;

    /**
     * The index of the column.
     */
    protected final int resultSetIndex;

    /**
     * The number of rows in this column.
     */
    protected final int numberOfRows;

    /**
     * The index of the first value mapped to a Java class.
     */
    protected int firstRetrievedIndex;

    /**
     * The index of the last value mapped to a Java class.
     */
    protected int lastRetrievedIndex;

	protected AbstractQueryResultSetColumn(String columnType, long tablePointer, int resultSetIndex, String columnName,
                                           int columnDigits, int columnScale, int numberOfRows) {
        super(columnType, columnName, columnDigits, columnScale);
        this.tablePointer = tablePointer;
        this.resultSetIndex = resultSetIndex;
        this.numberOfRows = numberOfRows;
        this.firstRetrievedIndex = numberOfRows;
        this.lastRetrievedIndex = 0;
 	}

    /**
     * Gets the number of rows in this column.
     *
     * @return The number of rows
     */
    public int getNumberOfRows() { return this.numberOfRows; }

    protected abstract void fetchMoreData(int startIndex, int numberOfRowsToRetrieve) throws MonetDBEmbeddedException;

    protected abstract A storeNewDataAndGetResult(int startIndex, int numberOfRowsToRetrieve);

    protected abstract boolean[] checkIfIndexesAreNullImplementation(A values, boolean[] res)
            throws MonetDBEmbeddedException;

    protected abstract Object[] mapValuesToObjectArrayImplementation(A values)
            throws MonetDBEmbeddedException;

    /**
     * Maps columns values using the provided Java representation by the query.
     *
     * @param startIndex The first column index to retrieve
     * @param endIndex The last column index to retrieve
     * @return The column values as a Java array
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public A fetchColumnValues(int startIndex, int endIndex) throws MonetDBEmbeddedException {
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
            throw new ArrayIndexOutOfBoundsException("Retrieving 0 values?");
        }
        boolean hasToFetch = false;
        int numberOfRowsToRetrieve = endIndex - startIndex;
        int firstIndexToFetch = Math.min(startIndex, this.firstRetrievedIndex);
        int lastIndexToFetch = Math.max(endIndex, this.lastRetrievedIndex);
        if(startIndex < this.firstRetrievedIndex) {
            this.firstRetrievedIndex = startIndex;
            hasToFetch = true;
        }
        if(endIndex > this.lastRetrievedIndex) {
            this.lastRetrievedIndex = endIndex;
            hasToFetch = true;
        }
        if(hasToFetch) {
            if(this.tablePointer == 0) {
                throw new MonetDBEmbeddedException("Connection closed!");
            }
            this.fetchMoreData(firstIndexToFetch, lastIndexToFetch);
        }
        return this.storeNewDataAndGetResult(startIndex, numberOfRowsToRetrieve);
    }

    /**
     * Maps columns values using the provided Java representation by the query asynchronously.
     *
     * @param startIndex The first column index to retrieve
     * @param endIndex The last column index to retrieve
     * @return The column values as a Java array
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    /*@SuppressWarnings("unchecked")
    public CompletableFuture<T[]> fetchColumnValuesAsync(int startIndex, int endIndex) throws MonetDBEmbeddedException {
        return this.fetchColumnValuesAsync(startIndex, endIndex, (Class<T>) this.mapping.getJavaClass());
    }*/

    /**
     * Maps the first N column values using the provided Java representation by the query.
     *
     * @param n The last column index to map
     * @return The column values as a Java array
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public A fetchFirstNColumnValues(int n) throws MonetDBEmbeddedException {
        return this.fetchColumnValues(0, n);
    }

    /**
     * Maps the first N column values using the provided Java representation by the query asynchronously.
     *
     * @param n The last column index to map
     * @return The column values as a Java array
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    /*public CompletableFuture<T[]> fetchFirstNColumnValuesAsync(int n) throws MonetDBEmbeddedException {
        return this.fetchColumnValuesAsync(0, n);
    }*/

    /**
     * Maps all column values using the provided Java representation by the query.
     *
     * @return The column values as a Java array
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public A fetchAllColumnValues() throws MonetDBEmbeddedException {
        return this.fetchColumnValues(0, this.numberOfRows);
    }

    /**
     * Maps all column values using the provided Java representation by the query asynchronously.
     *
     * @return The column values as a Java array
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    /*public CompletableFuture<T[]> fetchAllColumnValuesAsync() throws MonetDBEmbeddedException {
        return this.fetchColumnValuesAsync(0, this.numberOfRows);
    }*/

    /**
     * Checks if column indexes are null
     *
     * @param startIndex The first column index to check
     * @param endIndex The last column index to check
     * @return If the index is null or not
     */
    public boolean[] checkIfIndexesAreNull(int startIndex, int endIndex) throws MonetDBEmbeddedException {
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
            throw new ArrayIndexOutOfBoundsException("Checking 0 values?");
        }
        int numberOfRowsToRetrieve = endIndex - startIndex;
        A values = this.fetchColumnValues(startIndex, endIndex);
        boolean[] res = new boolean[numberOfRowsToRetrieve];
        return this.checkIfIndexesAreNullImplementation(values, res);
    }

    /**
     * Maps values to a Java Object array while mapping null values as well.
     *
     * @param startIndex The first column index to map
     * @param endIndex The last column index to map
     * @return The mapped Java array
     */
    public Object[] mapValuesToObjectArray(int startIndex, int endIndex) throws MonetDBEmbeddedException {
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
            throw new ArrayIndexOutOfBoundsException("Retrieving 0 values?");
        }
        A values = this.fetchColumnValues(startIndex, endIndex);
        return this.mapValuesToObjectArrayImplementation(values);
    }
}
