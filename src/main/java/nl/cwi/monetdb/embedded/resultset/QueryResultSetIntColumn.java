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
 * A MonetDB column converted to an array of Java integer values.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class QueryResultSetIntColumn extends AbstractQueryResultSetColumn<int[]> {

    /**
     * MonetDB's int null constant.
     */
    private static int IntNullConstant;

    /**
     * Gets MonetDB's int null constant
     *
     * @return MonetDB's int null constant
     */
    public static int GetIntNullConstant() { return IntNullConstant; }

    /**
     * Array with the retrieved values.
     */
    private final int[] values;

    protected QueryResultSetIntColumn(String columnType, long tablePointer, int resultSetIndex, String columnName,
                                      int columnDigits, int columnScale, int numberOfRows) {
        super(columnType, tablePointer, resultSetIndex, columnName, columnDigits, columnScale, numberOfRows);
        if(!this.getMapping().getJavaClass().equals(Integer.class)) {
            throw new ClassCastException("The parameter must be of integer type!!");
        }
        this.values = new int[numberOfRows];
    }

    @Override
    protected void fetchMoreData(int startIndex, int endIndex) throws MonetDBEmbeddedException {
        int[] values = this.fetchValuesInternal(this.tablePointer, this.resultSetIndex, startIndex, endIndex);
        System.arraycopy(values, 0, this.values, startIndex, values.length);
    }

    @Override
    protected int[] storeNewDataAndGetResult(int startIndex, int numberOfRowsToRetrieve) {
        int[] result = new int[numberOfRowsToRetrieve];
        System.arraycopy(this.values, startIndex, result, 0, numberOfRowsToRetrieve);
        return result;
    }

    @Override
    protected boolean[] checkIfIndexesAreNullImplementation(int[] values, boolean[] res) throws MonetDBEmbeddedException {
        for(int i = 0 ; i < values.length ; i++) {
            res[i] = (values[i] == IntNullConstant);
        }
        return res;
    }

    @Override
    protected Integer[] mapValuesToObjectArrayImplementation(int[] values) throws MonetDBEmbeddedException {
        Integer[] res = new Integer[values.length];
        for(int i = 0 ; i < values.length ; i++) {
            res[i] = (values[i] == IntNullConstant) ? null : values[i];
        }
        return res;
    }

    /**
     * Internal implementation to fetch values from the column.
     */
    private native int[] fetchValuesInternal(long tablePointer, int resultSetIndex, int startIndex, int endIndex)
            throws MonetDBEmbeddedException;
}
