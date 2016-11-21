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
 * A MonetDB column converted to an array of Java boolean values.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class QueryResultSetBooleanColumn extends AbstractQueryResultSetColumn<boolean[]> {

    /**
     * MonetDB's boolean null constant.
     */
    private static boolean BooleanNullConstant;

    /**
     * Gets MonetDB's boolean null constant
     *
     * @return MonetDB's boolean null constant
     */
    public static boolean GetBooleanNullConstant() { return BooleanNullConstant; }

    /**
     * Array with the retrieved values.
     */
    private final boolean[] values;

    protected QueryResultSetBooleanColumn(String columnType, long tablePointer, int resultSetIndex, String columnName,
                                          int columnDigits, int columnScale, int numberOfRows) {
        super(columnType, tablePointer, resultSetIndex, columnName, columnDigits, columnScale, numberOfRows);
        if(!this.getMapping().getJavaClass().equals(Boolean.class)) {
            throw new ClassCastException("The parameter must be of boolean type!!");
        }
        this.values = new boolean[numberOfRows];
    }

    @Override
    protected void fetchMoreData(int startIndex, int endIndex) throws MonetDBEmbeddedException {
        boolean[] values = this.fetchValuesInternal(this.tablePointer, this.resultSetIndex, startIndex, endIndex);
        System.arraycopy(values, 0, this.values, startIndex, values.length);
    }

    @Override
    protected boolean[] storeNewDataAndGetResult(int startIndex, int numberOfRowsToRetrieve) {
        boolean[] result = new boolean[numberOfRowsToRetrieve];
        System.arraycopy(this.values, startIndex, result, 0, numberOfRowsToRetrieve);
        return result;
    }

    @Override
    protected boolean[] checkIfIndexesAreNullImplementation(boolean[] values, boolean[] res) throws MonetDBEmbeddedException {
        for(int i = 0 ; i < values.length ; i++) {
            res[i] = (values[i] == BooleanNullConstant);
        }
        return res;
    }

    @Override
    protected Boolean[] mapValuesToObjectArrayImplementation(boolean[] values) throws MonetDBEmbeddedException {
        Boolean[] res = new Boolean[values.length];
        for(int i = 0 ; i < values.length ; i++) {
            res[i] = (values[i] == BooleanNullConstant) ? null : values[i];
        }
        return res;
    }

    /**
     * Internal implementation to fetch values from the column.
     */
    private native boolean[] fetchValuesInternal(long tablePointer, int resultSetIndex, int startIndex, int endIndex)
            throws MonetDBEmbeddedException;
}
