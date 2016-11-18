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
 * A MonetDB column converted to an array of Java short values.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class QueryResultSetShortColumn extends AbstractQueryResultSetColumn<short[]> {

    /**
     * MonetDB's short null constant.
     */
    private static long ShortNullConstant;

    /**
     * Gets MonetDB's short null constant
     *
     * @return MonetDB's short null constant
     */
    public static long GetShortNullConstant() { return ShortNullConstant; }

    /**
     * Array with the retrieved values.
     */
    private final short[] values;

    protected QueryResultSetShortColumn(String columnType, long tablePointer, int resultSetIndex, String columnName,
                                        int columnDigits, int columnScale, int numberOfRows) {
        super(columnType, tablePointer, resultSetIndex, columnName, columnDigits, columnScale, numberOfRows);
        if(!this.getMapping().getJavaClass().equals(Short.class)) {
            throw new ClassCastException("The parameter must be of boolean type!!");
        }
        this.values = new short[numberOfRows];
    }

    @Override
    protected void fetchMoreData(int startIndex, int endIndex) throws MonetDBEmbeddedException {
        short[] values = this.fetchValuesInternal(this.tablePointer, this.resultSetIndex, startIndex, endIndex);
        System.arraycopy(values, 0, this.values, startIndex, values.length);
    }

    @Override
    protected short[] storeNewDataAndGetResult(int startIndex, int numberOfRowsToRetrieve) {
        short[] result = new short[numberOfRowsToRetrieve];
        System.arraycopy(this.values, startIndex, result, 0, numberOfRowsToRetrieve);
        return result;
    }

    @Override
    protected boolean[] checkIfIndexesAreNullImplementation(short[] values, boolean[] res) throws MonetDBEmbeddedException {
        for(int i = 0 ; i < values.length ; i++) {
            res[i] = (values[i] == ShortNullConstant);
        }
        return res;
    }

    @Override
    protected Short[] mapValuesToObjectArrayImplementation(short[] values) throws MonetDBEmbeddedException {
        Short[] res = new Short[values.length];
        for(int i = 0 ; i < values.length ; i++) {
            res[i] = (values[i] == ShortNullConstant) ? null : values[i];
        }
        return res;
    }

    /**
     * Internal implementation to fetch values from the column.
     */
    private native short[] fetchValuesInternal(long tablePointer, int resultSetIndex, int startIndex, int endIndex)
            throws MonetDBEmbeddedException;
}
