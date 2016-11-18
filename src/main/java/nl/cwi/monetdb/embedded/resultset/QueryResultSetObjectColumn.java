/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded.resultset;

import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedException;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.ListIterator;

/**
 * A MonetDB column converted to an array of Java objects.
 *
 * @param <T> The Java class of the mapped MonetDB column
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class QueryResultSetObjectColumn<T> extends AbstractQueryResultSetColumn<T[]> implements Iterable<T> {

    /**
     * Array with the retrieved values.
     */
    private final T[] values;

    @SuppressWarnings("unchecked")
    public QueryResultSetObjectColumn(String columnType, long tablePointer, int resultSetIndex, String columnName,
                                      int columnDigits, int columnScale, int numberOfRows) {
        super(columnType, tablePointer, resultSetIndex, columnName, columnDigits, columnScale, numberOfRows);
        this.values = (T[]) Array.newInstance(this.mapping.getJavaClass(), numberOfRows);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void fetchMoreData(int startIndex, int endIndex) throws MonetDBEmbeddedException {
        T[] values = this.fetchValuesInternal(this.tablePointer, this.resultSetIndex,
                (Class<T>) this.mapping.getJavaClass(), this.mapping.ordinal(), startIndex, endIndex);
        System.arraycopy(values, 0, this.values, startIndex, values.length);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected T[] storeNewDataAndGetResult(int startIndex, int numberOfRowsToRetrieve) {
        T[] result = (T[]) Array.newInstance(this.mapping.getJavaClass(), numberOfRowsToRetrieve);
        System.arraycopy(this.values, startIndex, result, 0, numberOfRowsToRetrieve);
        return result;
    }

    @Override
    protected boolean[] checkIfIndexesAreNullImplementation(T[] values, boolean[] res) throws MonetDBEmbeddedException {
        for(int i = 0 ; i < values.length ; i++) {
            res[i] = (values[i] == null);
        }
        return res;
    }

    @Override
    protected T[] mapValuesToObjectArrayImplementation(T[] values) throws MonetDBEmbeddedException {
        return values;
    }

    @Override
    public ListIterator<T> iterator() {
        try {
            return Arrays.asList(this.fetchAllColumnValues()).listIterator();
        } catch (Exception ex) {
            return null;
        }
    }

    /**
     * Internal implementation to fetch values from the column.
     */
    private native T[] fetchValuesInternal(long tablePointer, int resultSetIndex, Class<T> jClass, int javaIndex,
                                           int startIndex, int endIndex) throws MonetDBEmbeddedException;
}
