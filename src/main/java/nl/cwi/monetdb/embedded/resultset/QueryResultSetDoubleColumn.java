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
 * A MonetDB column converted to an array of Java double values.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public final class QueryResultSetDoubleColumn extends AbstractQueryResultSetColumn<double[]> {

    /**
     * Gets MonetDB's double null constant
     *
     * @return MonetDB's double null constant
     */
    public static native double GetDoubleNullConstant();

    /**
     * Checks if the double value is null or not.
     *
     * @param value The value to evaluate
     * @return If the double value is null or not.
     */
    public static native boolean CheckDoubleIsNull(double value);

    /**
     * Array with the retrieved values.
     */
    private final double[] values;

    protected QueryResultSetDoubleColumn(String columnType, long tablePointer, int resultSetIndex, String columnName,
                                         int columnDigits, int columnScale, int numberOfRows) {
        super(columnType, tablePointer, resultSetIndex, columnName, columnDigits, columnScale, numberOfRows);
        if(!this.getMapping().getJavaClass().equals(Double.class)) {
            throw new ClassCastException("The parameter must be of double type!!");
        }
        this.values = new double[numberOfRows];
    }

    @Override
    protected void fetchMoreData(int startIndex, int endIndex) throws MonetDBEmbeddedException {
        this.fetchValuesInternal(this.tablePointer, this.resultSetIndex, startIndex, endIndex, this.values, this.nullValues);
    }

    @Override
    protected double[] storeNewDataAndGetResult(int startIndex, int numberOfRowsToRetrieve) {
        double[] result = new double[numberOfRowsToRetrieve];
        System.arraycopy(this.values, startIndex, result, 0, numberOfRowsToRetrieve);
        return result;
    }

    @Override
    protected Double[] mapValuesToObjectArrayImplementation(int startIndex, int numberOfRowsToRetrieve) {
        Double[] res = new Double[numberOfRowsToRetrieve];
        int endIndex = startIndex + numberOfRowsToRetrieve;
        for(int i = startIndex, j = 0 ; i < endIndex ; i++, j++) {
            res[j] = (this.nullValues[i]) ? null : this.values[i];
        }
        return res;
    }

    /**
     * Internal implementation to fetch values from the column.
     */
    private native void fetchValuesInternal(long tablePointer, int resultSetIndex, int startIndex, int endIndex,
                                            double[] values, boolean[] nullValues) throws MonetDBEmbeddedException;
}
