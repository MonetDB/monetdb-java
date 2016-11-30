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
 * A MonetDB column converted to an array of Java float values.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public final class QueryResultSetFloatColumn extends AbstractQueryResultSetColumn<float[]> {

    /**
     * Gets MonetDB's float null constant
     *
     * @return MonetDB's float null constant
     */
    public static native float GetFloatNullConstant();

    /**
     * Checks if the float value is null or not.
     *
     * @param value The value to evaluate
     * @return If the float value is null or not.
     */
    public static native boolean CheckFloatIsNull(float value);

    /**
     * Array with the retrieved values.
     */
    private final float[] values;

    protected QueryResultSetFloatColumn(String columnType, long tablePointer, int resultSetIndex, String columnName,
                                        int columnDigits, int columnScale, int numberOfRows) {
        super(columnType, tablePointer, resultSetIndex, columnName, columnDigits, columnScale, numberOfRows);
        if(!this.getMapping().getJavaClass().equals(Float.class)) {
            throw new ClassCastException("The parameter must be of float type!!");
        }
        this.values = new float[numberOfRows];
    }

    @Override
    protected void fetchMoreData(int startIndex, int endIndex) throws MonetDBEmbeddedException {
        float[] values = this.fetchValuesInternal(this.tablePointer, this.resultSetIndex, startIndex, endIndex);
        System.arraycopy(values, 0, this.values, startIndex, values.length);
    }

    @Override
    protected float[] storeNewDataAndGetResult(int startIndex, int numberOfRowsToRetrieve) {
        float[] result = new float[numberOfRowsToRetrieve];
        System.arraycopy(this.values, startIndex, result, 0, numberOfRowsToRetrieve);
        return result;
    }

    @Override
    protected boolean[] checkIfIndexesAreNullImplementation(float[] values, boolean[] res) throws MonetDBEmbeddedException {
        float nil = GetFloatNullConstant();
        for(int i = 0 ; i < values.length ; i++) {
            res[i] = (values[i] == nil);
        }
        return res;
    }

    @Override
    protected Float[] mapValuesToObjectArrayImplementation(float[] values) throws MonetDBEmbeddedException {
        float nil = GetFloatNullConstant();
        Float[] res = new Float[values.length];
        for(int i = 0 ; i < values.length ; i++) {
            res[i] = (values[i] == nil) ? null : values[i];
        }
        return res;
    }

    /**
     * Internal implementation to fetch values from the column.
     */
    private native float[] fetchValuesInternal(long tablePointer, int resultSetIndex, int startIndex, int endIndex)
            throws MonetDBEmbeddedException;
}
