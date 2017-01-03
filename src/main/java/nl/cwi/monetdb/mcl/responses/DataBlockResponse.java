/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.responses;

import nl.cwi.monetdb.jdbc.MonetBlob;
import nl.cwi.monetdb.jdbc.MonetClob;
import nl.cwi.monetdb.mcl.protocol.AbstractProtocol;
import nl.cwi.monetdb.mcl.protocol.ProtocolException;
import nl.cwi.monetdb.mcl.protocol.ServerResponses;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Types;
import java.util.Arrays;
import java.util.Calendar;

/**
 * The DataBlockResponse is tabular data belonging to a
 * ResultSetResponse.  Tabular data from the server typically looks
 * like:
 * <pre>
 * [ "value",	56	]
 * </pre>
 * where each column is separated by ",\t" and each tuple surrounded
 * by brackets ("[" and "]").  A DataBlockResponse object holds the
 * raw data as read from the server, in a parsed manner, ready for
 * easy retrieval.
 *
 * This object is not intended to be queried by multiple threads
 * synchronously. It is designed to work for one thread retrieving
 * rows from it.  When multiple threads will retrieve rows from this
 * object, it is possible for threads to get the same data.
 */
public class DataBlockResponse implements IIncompleteResponse {

    /** The array to keep the data in */
    private Object[] data;
    /** The counter which keeps the current position in the data array */
    private int pos;
    /** The connection protocol to parse the tuple lines */
    private final AbstractProtocol protocol;
    /** The JdbcSQLTypes mapping */
    private final int[] jdbcSQLTypes;
    /** A mapping of null values of the current Row */
    private boolean[][] nullMappings;
    /** A 'pointer' to the current line */
    private int blockLine;

    /**
     * Constructs a DataBlockResponse object.
     *
     * @param rowcount the number of rows
     * @param columncount the number of columns
     */
    DataBlockResponse(int rowcount, int columncount, AbstractProtocol protocol, int[] JdbcSQLTypes) {
        this.pos = -1;
        this.data = new Object[columncount];
        this.nullMappings = new boolean[rowcount][columncount];
        this.protocol = protocol;
        this.jdbcSQLTypes = JdbcSQLTypes;
    }

    /**
     * addLine adds a String of data to this object's data array. Note that an IndexOutOfBoundsException can be thrown
     * when an attempt is made to add more than the original construction size specified.
     *
     * @param protocol The connection's protocol
     * @throws ProtocolException If the result line is not expected
     */
    @Override
    public void addLines(AbstractProtocol protocol) throws ProtocolException {
        if (protocol.getCurrentServerResponseHeader() != ServerResponses.RESULT) {
            throw new ProtocolException("protocol violation: unexpected line in data block: " +
                    protocol.getRemainingStringLine(0));
        }

        if(this.pos == -1) { //if it's the first line, initialize the matrix
            int numberOfColumns = this.data.length, numberOfRows = this.nullMappings.length;
            for (int i = 0 ; i < numberOfColumns ; i++) {
                switch (this.jdbcSQLTypes[i]) {
                    case Types.BOOLEAN:
                        this.data[i] = new boolean[numberOfRows];
                        break;
                    case Types.TINYINT:
                        this.data[i] = new byte[numberOfRows];
                        break;
                    case Types.SMALLINT:
                        this.data[i] = new short[numberOfRows];
                        break;
                    case Types.INTEGER:
                        this.data[i] = new int[numberOfRows];
                        break;
                    case Types.BIGINT:
                        this.data[i] = new long[numberOfRows];
                        break;
                    case Types.REAL:
                        this.data[i] = new float[numberOfRows];
                        break;
                    case Types.DOUBLE:
                        this.data[i] = new double[numberOfRows];
                        break;
                    case Types.DECIMAL:
                        this.data[i] = new BigDecimal[numberOfRows];
                        break;
                    case Types.NUMERIC:
                        this.data[i] = new BigInteger[numberOfRows];
                        break;
                    case Types.BLOB:
                        this.data[i] = new MonetBlob[numberOfRows];
                        break;
                    case Types.CLOB:
                        this.data[i] = new MonetClob[numberOfRows];
                        break;
                    case Types.TIME:
                    case Types.TIME_WITH_TIMEZONE:
                    case Types.DATE:
                    case Types.TIMESTAMP:
                    case Types.TIMESTAMP_WITH_TIMEZONE:
                        this.data[i] = new Calendar[numberOfRows];
                        break;
                    case Types.LONGVARBINARY:
                        this.data[i] = new byte[numberOfRows][];
                        break;
                    default: //CHAR, VARCHAR, OTHER
                        this.data[i] = new String[numberOfRows];
                }
            }
        }

        // add to the backing array
        int nextPos = this.pos + 1;
        this.pos = this.protocol.parseTupleLines(nextPos, this.jdbcSQLTypes, this.data, this.nullMappings);
    }

    /**
     * Returns whether this Response expects more lines to be added to it.
     *
     * @return true if a next line should be added, false otherwise
     */
    @Override
    public boolean wantsMore() {
        // remember: pos is the value already stored
        return (this.pos + 1) < this.nullMappings.length;
    }

    /**
     * Instructs the Response implementation to close and do the necessary clean up procedures.
     */
    @Override
    public void close() {
        // feed all rows to the garbage collector
        int numberOfColumns = this.data.length;
        for (int i = 0; i < numberOfColumns; i++) {
            data[i] = null;
            nullMappings[i] = null;
        }
        data = null;
        nullMappings = null;
    }

    /* Methods to be called after the block construction has been completed */

    void setBlockLine(int blockLine) {
        this.blockLine = blockLine;
    }

    public void setData(Object[] data) { /* For VirtualResultSet :( */
        this.data = data;
    }

    public Object[] getData() { /* For VirtualResultSet :( */
        return this.data;
    }

    public boolean checkValueIsNull(int column) {
        return this.nullMappings[this.blockLine][column];
    }

    public boolean getBooleanValue(int column) {
        return ((boolean[]) this.data[column])[this.blockLine];
    }

    public byte getByteValue(int column) {
        return ((byte[]) this.data[column])[this.blockLine];
    }

    public short getShortValue(int column) {
        return ((short[]) this.data[column])[this.blockLine];
    }

    public int getIntValue(int column) {
        return ((int[]) this.data[column])[this.blockLine];
    }

    public long getLongValue(int column) {
        return ((long[]) this.data[column])[this.blockLine];
    }

    public float getFloatValue(int column) {
        return ((float[]) this.data[column])[this.blockLine];
    }

    public double getDoubleValue(int column) {
        return ((double[]) this.data[column])[this.blockLine];
    }

    public Object getObjectValue(int column) {
        return ((Object[]) this.data[column])[this.blockLine];
    }

    public String getValueAsString(int column) {
        switch (this.jdbcSQLTypes[column]) {
            case Types.BOOLEAN:
                return Boolean.toString(((boolean[]) this.data[column])[this.blockLine]);
            case Types.TINYINT:
                return Byte.toString(((byte[]) this.data[column])[this.blockLine]);
            case Types.SMALLINT:
                return Short.toString(((short[]) this.data[column])[this.blockLine]);
            case Types.INTEGER:
                return Integer.toString(((int[]) this.data[column])[this.blockLine]);
            case Types.BIGINT:
                return Long.toString(((long[]) this.data[column])[this.blockLine]);
            case Types.REAL:
                return Float.toString(((float[]) this.data[column])[this.blockLine]);
            case Types.DOUBLE:
                return Double.toString(((double[]) this.data[column])[this.blockLine]);
            case Types.LONGVARBINARY:
                return Arrays.toString(((byte[][]) this.data[column])[this.blockLine]);
            default: //CHAR, VARCHAR, LONGVARCHAR, OTHER, BLOB, CLOB and others
                return ((Object[]) this.data[column])[this.blockLine].toString();
        }
    }

    public Object getValueAsObject(int column) {
        switch (this.jdbcSQLTypes[column]) {
            case Types.BOOLEAN:
                return ((boolean[]) this.data[column])[this.blockLine];
            case Types.TINYINT:
                return ((byte[]) this.data[column])[this.blockLine];
            case Types.SMALLINT:
                return ((short[]) this.data[column])[this.blockLine];
            case Types.INTEGER:
                return ((int[]) this.data[column])[this.blockLine];
            case Types.BIGINT:
                return ((long[]) this.data[column])[this.blockLine];
            case Types.REAL:
                return ((float[]) this.data[column])[this.blockLine];
            case Types.DOUBLE:
                return ((double[]) this.data[column])[this.blockLine];
            default:
                return ((Object[]) this.data[column])[this.blockLine];
        }
    }
}
