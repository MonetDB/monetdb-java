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
 * A MonetDB column converted to an array of Java byte values.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class QueryResultSetByteColumn extends AbstractQueryResultSetColumn<byte[]> {

    /**
     * MonetDB's byte null constant.
     */
    private static byte ByteNullConstant;

    /**
     * Gets MonetDB's byte null constant
     *
     * @return MonetDB's byte null constant
     */
    public static byte GetByteNullConstant() { return ByteNullConstant; }

    /**
     * Array with the retrieved values.
     */
    private final byte[] values;

    protected QueryResultSetByteColumn(String columnType, long tablePointer, int resultSetIndex, String columnName,
                                       int columnDigits, int columnScale, int numberOfRows) {
        super(columnType, tablePointer, resultSetIndex, columnName, columnDigits, columnScale, numberOfRows);
        if(!this.getMapping().getJavaClass().equals(Byte.class)) {
            throw new ClassCastException("The parameter must be of byte type!!");
        }
        this.values = new byte[numberOfRows];
    }

    @Override
    protected void fetchMoreData(int startIndex, int endIndex) throws MonetDBEmbeddedException {
        byte[] values = this.fetchValuesInternal(this.tablePointer, this.resultSetIndex, startIndex, endIndex);
        System.arraycopy(values, 0, this.values, startIndex, values.length);
    }

    @Override
    protected byte[] storeNewDataAndGetResult(int startIndex, int numberOfRowsToRetrieve) {
        byte[] result = new byte[numberOfRowsToRetrieve];
        System.arraycopy(this.values, startIndex, result, 0, numberOfRowsToRetrieve);
        return result;
    }

    @Override
    protected boolean[] checkIfIndexesAreNullImplementation(byte[] values, boolean[] res) throws MonetDBEmbeddedException {
        for(int i = 0 ; i < values.length ; i++) {
            res[i] = (values[i] == ByteNullConstant);
        }
        return res;
    }

    @Override
    protected Byte[] mapValuesToObjectArrayImplementation(byte[] values) throws MonetDBEmbeddedException {
        Byte[] res = new Byte[values.length];
        for(int i = 0 ; i < values.length ; i++) {
            res[i] = (values[i] == ByteNullConstant) ? null : values[i];
        }
        return res;
    }

    /**
     * Internal implementation to fetch values from the column.
     */
    private native byte[] fetchValuesInternal(long tablePointer, int resultSetIndex, int startIndex, int endIndex)
            throws MonetDBEmbeddedException;
}
