/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.ListIterator;

/**
 *  Am abstract class for accessing, 
 *  materialised (Java-level) query result columns.
 *
 * @param <T> A Java class mapped to a MonetDB data type
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class QueryResultSetColumn<T> extends AbstractColumn<T> {

    /**
     * The C pointer of the result set of the column.
     */
    protected final long resultSetPointer;

    /**
     * Array with the retrieved values.
     */
    private final T[] values;

    /**
     * The index of the first value mapped to a Java class.
     */
    private int firstRetrievedIndex;

    /**
     * The index of the last value mapped to a Java class.
     */
    private int lastRetrievedIndex;

    @SuppressWarnings("unchecked")
	protected QueryResultSetColumn(int resultSetIndex, int numberOfRows, String columnName, String columnType,
                                   int columnDigits, int columnScale, long resultSetPointer) {
        super(resultSetIndex, numberOfRows, columnName, columnType, columnDigits, columnScale);
        this.resultSetPointer = resultSetPointer;
        this.firstRetrievedIndex = numberOfRows;
        this.lastRetrievedIndex = 0;
        this.values = (T[]) Array.newInstance(this.mapping.getJavaClass(), numberOfRows);
 	}

    /**
     * Maps columns values into the Java representation.
     *
     * @param startIndex The first column index to retrieve
     * @param endIndex The last column index to retrieve
     * @param javaClass The Java class to map
     * @return The column values as a Java array
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    @SuppressWarnings("unchecked")
    public T[] fetchColumnValues(int startIndex, int endIndex, Class<T> javaClass) throws MonetDBEmbeddedException {
        if(endIndex < startIndex) {
            int aux = startIndex;
            startIndex = endIndex;
            endIndex = aux;
        }
        int numberOfRowsToRetrieve = endIndex - startIndex;
        if (startIndex < 0) {
            throw new ArrayIndexOutOfBoundsException("The start index must be larger than 0!");
        } else if (endIndex > this.numberOfRows) {
            throw new ArrayIndexOutOfBoundsException("The index must be smaller than the number of elements in the columns!");
        } else if(startIndex == endIndex) {
            throw new ArrayIndexOutOfBoundsException("Retrieving 0 values?");
        }
        if(startIndex < this.firstRetrievedIndex) {
            if(this.resultSetPointer == 0) {
                throw new MonetDBEmbeddedException("Connection closed!");
            }
            if(startIndex < this.firstRetrievedIndex) {
                T[] new_start_batch = this.fetchValuesInternal(this.resultSetPointer, this.resultSetIndex,
                        (Class<T>) this.mapping.getJavaClass(), this.mapping.ordinal(), startIndex, this.firstRetrievedIndex);
                System.arraycopy(new_start_batch, 0, this.values, startIndex, new_start_batch.length);
                this.firstRetrievedIndex = startIndex;
            }
        }
        if(endIndex > this.lastRetrievedIndex) {
            if(this.resultSetPointer == 0) {
                throw new MonetDBEmbeddedException("Connection closed!");
            }
            if(endIndex > this.lastRetrievedIndex) {
                T[] new_end_batch = this.fetchValuesInternal(this.resultSetPointer, this.resultSetIndex,
                        (Class<T>) this.mapping.getJavaClass(), this.mapping.ordinal(), this.lastRetrievedIndex, endIndex);
                System.arraycopy(new_end_batch, 0, this.values, this.lastRetrievedIndex, new_end_batch.length);
                this.lastRetrievedIndex = endIndex;
            }
        }
        T[] result = (T[]) Array.newInstance(javaClass, numberOfRowsToRetrieve);
        System.arraycopy(this.values, startIndex, result, 0, numberOfRowsToRetrieve);
        return result;
    }

    /**
     * Maps columns values into the Java representation asynchronously.
     *
     * @param startIndex The first column index to retrieve
     * @param endIndex The last column index to retrieve
     * @param javaClass The Java class to map
     * @return The column values as a Java array
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public T[] fetchColumnValuesAsync(int startIndex, int endIndex, Class<T> javaClass) throws MonetDBEmbeddedException {
        /* CompletableFuture.supplyAsync(() -> this.fetchColumnValues(startIndex, endIndex, javaClass)); */
        throw new UnsupportedOperationException("Must wait for Java 8 :(");
    }

    /**
     * Maps the first N column values.
     *
     * @param n The last column index to map
     * @param javaClass The Java class to map
     * @return The column values as a Java array
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public T[] fetchFirstNColumnValues(int n, Class<T> javaClass) throws MonetDBEmbeddedException {
        return this.fetchColumnValues(0, n, javaClass);
    }

    /**
     * Maps the first N column values asynchronously.
     *
     * @param n The last column index to map
     * @param javaClass The Java class to map
     * @return The column values as a Java array
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public T[] fetchFirstNColumnValuesAsync(int n, Class<T> javaClass) throws MonetDBEmbeddedException {
        return this.fetchColumnValuesAsync(0, n, javaClass);
    }

    /**
     * Maps all column values.
     *
     * @param javaClass The Java class to map
     * @return The column values as a Java array
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public T[] fetchAllColumnValues(Class<T> javaClass) throws MonetDBEmbeddedException {
        return this.fetchColumnValues(0, this.numberOfRows, javaClass);
    }

    /**
     * Maps all column values asynchronously.
     *
     * @param javaClass The Java class to map
     * @return The column values as a Java array
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public T[] fetchAllColumnValuesAsync(Class<T> javaClass) throws MonetDBEmbeddedException {
        return this.fetchColumnValuesAsync(0, this.numberOfRows, javaClass);
    }

    /**
     * Maps columns values using the provided Java representation by the query.
     *
     * @param startIndex The first column index to retrieve
     * @param endIndex The last column index to retrieve
     * @return The column values as a Java array
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    @SuppressWarnings("unchecked")
    public T[] fetchColumnValues(int startIndex, int endIndex) throws MonetDBEmbeddedException {
        return this.fetchColumnValues(startIndex, endIndex, (Class<T>) this.mapping.getJavaClass());
    }

    /**
     * Maps columns values using the provided Java representation by the query asynchronously.
     *
     * @param startIndex The first column index to retrieve
     * @param endIndex The last column index to retrieve
     * @return The column values as a Java array
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    @SuppressWarnings("unchecked")
    public T[] fetchColumnValuesAsync(int startIndex, int endIndex) throws MonetDBEmbeddedException {
        return this.fetchColumnValuesAsync(startIndex, endIndex, (Class<T>) this.mapping.getJavaClass());
    }

    /**
     * Maps the first N column values using the provided Java representation by the query.
     *
     * @param n The last column index to map
     * @return The column values as a Java array
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public T[] fetchFirstNColumnValues(int n) throws MonetDBEmbeddedException {
        return this.fetchColumnValues(0, n);
    }

    /**
     * Maps the first N column values using the provided Java representation by the query asynchronously.
     *
     * @param n The last column index to map
     * @return The column values as a Java array
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public T[] fetchFirstNColumnValuesAsync(int n) throws MonetDBEmbeddedException {
        return this.fetchColumnValuesAsync(0, n);
    }

    /**
     * Maps all column values using the provided Java representation by the query.
     *
     * @return The column values as a Java array
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public T[] fetchAllColumnValues() throws MonetDBEmbeddedException {
        return this.fetchColumnValues(0, this.numberOfRows);
    }

    /**
     * Maps all column values using the provided Java representation by the query asynchronously.
     *
     * @return The column values as a Java array
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public T[] fetchAllColumnValuesAsync() throws MonetDBEmbeddedException {
        return this.fetchColumnValuesAsync(0, this.numberOfRows);
    }

    @Override
    public ListIterator<T> iterator() {
        return Arrays.asList(this.values).listIterator();
    }

    private native T[] fetchValuesInternal(long resultPointer, int resultSetIndex, Class<T> jclass, int enumEntry,
                                           int first, int last) throws MonetDBEmbeddedException;

}
