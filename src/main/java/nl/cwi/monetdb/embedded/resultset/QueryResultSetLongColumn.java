/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded.resultset;

import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedException;

/**
 * A MonetDB column converted to an array of Java long values.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public final class QueryResultSetLongColumn extends AbstractQueryResultSetColumn<long[]> {

    /**
     * Gets MonetDB's long null constant
     *
     * @return MonetDB's long null constant
     */
    public static native long GetLongNullConstant();

    /**
     * Checks if the long value is null or not.
     *
     * @param value The value to evaluate
     * @return If the long value is null or not.
     */
    public static native boolean CheckLongIsNull(long value);

    /**
     * Array with the retrieved values.
     */
    private final long[] values;

    protected QueryResultSetLongColumn(String columnType, long tablePointer, int resultSetIndex, String columnName,
                                       int columnDigits, int columnScale, int numberOfRows) {
        super(columnType, tablePointer, resultSetIndex, columnName, columnDigits, columnScale, numberOfRows);
        if(!this.getMapping().getJavaClass().equals(Long.class)) {
            throw new ClassCastException("The parameter must be of long type!!");
        }
        this.values = new long[numberOfRows];
    }

    @Override
    protected void fetchMoreData(int startIndex, int endIndex) throws MonetDBEmbeddedException {
        long[] values = this.fetchValuesInternal(this.tablePointer, this.resultSetIndex, startIndex, endIndex);
        System.arraycopy(values, 0, this.values, startIndex, values.length);
    }

    @Override
    protected long[] storeNewDataAndGetResult(int startIndex, int numberOfRowsToRetrieve) {
        long[] result = new long[numberOfRowsToRetrieve];
        System.arraycopy(this.values, startIndex, result, 0, numberOfRowsToRetrieve);
        return result;
    }

    @Override
    protected boolean[] checkIfIndexesAreNullImplementation(long[] values, boolean[] res) throws MonetDBEmbeddedException {
        long nil = GetLongNullConstant();
        for(int i = 0 ; i < values.length ; i++) {
            res[i] = (values[i] == nil);
        }
        return res;
    }

    @Override
    protected Long[] mapValuesToObjectArrayImplementation(long[] values) throws MonetDBEmbeddedException {
        long nil = GetLongNullConstant();
        Long[] res = new Long[values.length];
        for(int i = 0 ; i < values.length ; i++) {
            res[i] = (values[i] == nil) ? null : values[i];
        }
        return res;
    }

    /**
     * Internal implementation to fetch values from the column.
     */
    private native long[] fetchValuesInternal(long tablePointer, int resultSetIndex, int startIndex, int endIndex)
            throws MonetDBEmbeddedException;
}
