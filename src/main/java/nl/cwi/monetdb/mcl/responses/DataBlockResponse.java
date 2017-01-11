/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
 * The DataBlockResponse is tabular data belonging to a ResultSetResponse. On a MAPI connection, tabular data from the
 * server typically looks like:
 * <pre>
 * [ "value",	56	]
 * </pre>
 * where each column is separated by ",\t" and each tuple surrounded by brackets ("[" and "]"). A DataBlockResponse
 * object holds the raw data as read from the server, in a parsed manner, ready for easy retrieval. Meanwhile on an
 * Embedded connection, the data is automatically parsed.
 *
 * This object is not intended to be queried by multiple threads synchronously. It is designed to work for one thread
 * retrieving rows from it. When multiple threads will retrieve rows from this object, it is possible for threads to
 * get the same data.
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
    /** A 'pointer' to the current line */
    private int blockLine;
    /** The number of rows in the block */
    private final int rowcount;

    /**
     * Constructs a DataBlockResponse object.
     *
     * @param rowcount the number of rows
     * @param columncount the number of columns
     * @param protocol the underlying protocol
     * @param JdbcSQLTypes an array of the JDBC mappings of the columns
     */
    DataBlockResponse(int rowcount, int columncount, AbstractProtocol protocol, int[] JdbcSQLTypes) {
        this.pos = -1;
        this.rowcount = rowcount;
        this.data = new Object[columncount];
        this.protocol = protocol;
        this.jdbcSQLTypes = JdbcSQLTypes;
    }

    /**
     * addLines adds a batch of rows to the block. Before adding the first line, the column arrays are allocated.
     *
     * @param protocol The connection's protocol to fetch data from
     * @throws ProtocolException If the result line is not expected
     */
    @Override
    public void addLines(AbstractProtocol protocol) throws ProtocolException {
        int csrh = protocol.getCurrentServerResponse();
        if (csrh != ServerResponses.RESULT) {
            throw new ProtocolException("protocol violation: unexpected line in data block: " +
                    protocol.getRemainingStringLine(0));
        }

        if(this.pos == -1) { //if it's the first line, initialize the matrix
            int numberOfColumns = this.data.length;
            for (int i = 0 ; i < numberOfColumns ; i++) {
                switch (this.jdbcSQLTypes[i]) {
                    case Types.BOOLEAN:
                    case Types.TINYINT:
                        this.data[i] = new byte[this.rowcount];
                        break;
                    case Types.SMALLINT:
                        this.data[i] = new short[this.rowcount];
                        break;
                    case Types.INTEGER:
                        this.data[i] = new int[this.rowcount];
                        break;
                    case Types.BIGINT:
                        this.data[i] = new long[this.rowcount];
                        break;
                    case Types.REAL:
                        this.data[i] = new float[this.rowcount];
                        break;
                    case Types.DOUBLE:
                        this.data[i] = new double[this.rowcount];
                        break;
                    case Types.DECIMAL:
                        this.data[i] = new BigDecimal[this.rowcount];
                        break;
                    case Types.NUMERIC:
                        this.data[i] = new BigInteger[this.rowcount];
                        break;
                    case Types.BLOB:
                        this.data[i] = new MonetBlob[this.rowcount];
                        break;
                    case Types.CLOB:
                        this.data[i] = new MonetClob[this.rowcount];
                        break;
                    case Types.TIME:
                    case Types.TIME_WITH_TIMEZONE:
                    case Types.DATE:
                    case Types.TIMESTAMP:
                    case Types.TIMESTAMP_WITH_TIMEZONE:
                        this.data[i] = new Calendar[this.rowcount];
                        break;
                    case Types.LONGVARBINARY:
                        this.data[i] = new byte[this.rowcount][];
                        break;
                    default: //CHAR, VARCHAR, OTHER
                        this.data[i] = new String[this.rowcount];
                }
            }
        }

        // add to the backing array
        int nextPos = this.pos + 1;
        this.pos = this.protocol.parseTupleLines(nextPos, this.jdbcSQLTypes, this.data);
    }

    /**
     * Returns whether this Response expects more lines to be added to it.
     *
     * @return true if a next line should be added, false otherwise
     */
    @Override
    public boolean wantsMore() {
        // remember: pos is the value already stored
        return (this.pos + 1) < this.rowcount;
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
        }
        data = null;
    }

    /* Methods to be called after the block construction has been completed */

    /**
     * Sets the current line number on the block.
     *
     * @param blockLine the block line number
     */
    void setBlockLine(int blockLine) {
        this.blockLine = blockLine;
    }

    /**
     * Sets the data on the block.
     * This method is called by the MonetVirtualResultSet class which should be eliminated on the future.
     *
     * @param data the data to set
     */
    public void setData(Object[] data) { /* For VirtualResultSet :( */
        this.data = data;
    }

    /**
     * Gets the data on the block.
     * This method is called by the MonetVirtualResultSet class which should be eliminated on the future.
     *
     * @return the result set data
     */
    public Object[] getData() { /* For VirtualResultSet :( */
        return this.data;
    }

    /**
     * Checks if a value in the current row is null.
     *
     * @param column The column index starting from 0
     * @return If the value is null or not.
     */
    public boolean checkValueIsNull(int column) {
        switch (this.jdbcSQLTypes[column]) {
            case Types.BOOLEAN:
            case Types.TINYINT:
                return ((byte[]) this.data[column])[this.blockLine] == Byte.MIN_VALUE;
            case Types.SMALLINT:
                return ((short[]) this.data[column])[this.blockLine] == Short.MIN_VALUE;
            case Types.INTEGER:
                return ((int[]) this.data[column])[this.blockLine] == Integer.MIN_VALUE;
            case Types.BIGINT:
                return ((long[]) this.data[column])[this.blockLine] == Long.MIN_VALUE;
            case Types.REAL:
                return ((float[]) this.data[column])[this.blockLine] == Float.MIN_VALUE;
            case Types.DOUBLE:
                return ((double[]) this.data[column])[this.blockLine] == Double.MIN_VALUE;
            default:
                return ((Object[]) this.data[column])[this.blockLine] == null;
        }
    }

    /**
     * Gets the current row value as a Java Boolean.
     *
     * @param column The column index starting from 0
     * @return A Java Boolean if the column is a boolean, otherwise a ClassCastException is thrown
     */
    public boolean getBooleanValue(int column) {
        return ((byte[]) this.data[column])[this.blockLine] == 1;
    }

    /**
     * Gets the current row value as a Java Byte.
     *
     * @param column The column index starting from 0
     * @return A Java Byte if the column is a tinyint, otherwise a ClassCastException is thrown
     */
    public byte getByteValue(int column) {
        return ((byte[]) this.data[column])[this.blockLine];
    }

    /**
     * Gets the current row value as a Java Short.
     *
     * @param column The column index starting from 0
     * @return A Java Short if the column is a smallint, otherwise a ClassCastException is thrown
     */
    public short getShortValue(int column) {
        return ((short[]) this.data[column])[this.blockLine];
    }

    /**
     * Gets the current row value as a Java Integer.
     *
     * @param column The column index starting from 0
     * @return A Java Integer if the column is an integer or month_interval, otherwise a ClassCastException is thrown
     */
    public int getIntValue(int column) {
        return ((int[]) this.data[column])[this.blockLine];
    }

    /**
     * Gets the current row value as a Java Long.
     *
     * @param column The column index starting from 0
     * @return A Java Long if the column is a bigint or second_interval, otherwise a ClassCastException is thrown
     */
    public long getLongValue(int column) {
        return ((long[]) this.data[column])[this.blockLine];
    }

    /**
     * Gets the current row value as a Java Float.
     *
     * @param column The column index starting from 0
     * @return A Java Float if the column is a real, otherwise a ClassCastException is thrown
     */
    public float getFloatValue(int column) {
        return ((float[]) this.data[column])[this.blockLine];
    }

    /**
     * Gets the current row value as a Java Double.
     *
     * @param column The column index starting from 0
     * @return A Java Double if the column is a double, otherwise a ClassCastException is thrown
     */
    public double getDoubleValue(int column) {
        return ((double[]) this.data[column])[this.blockLine];
    }

    /**
     * Gets the current row value as a Java Object.
     *
     * @param column The column index starting from 0
     * @return A Java Object if the column is not a primitive type, otherwise a ClassCastException is thrown
     */
    public Object getObjectValue(int column) {
        return ((Object[]) this.data[column])[this.blockLine];
    }

    /**
     * Gets the current row value as a Java String.
     *
     * @param column The column index starting from 0
     * @return The String representation of the data type
     */
    public String getValueAsString(int column) {
        switch (this.jdbcSQLTypes[column]) {
            case Types.BOOLEAN:
                return ((byte[]) this.data[column])[this.blockLine] == 1 ? "true" : "false";
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
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.OTHER:
                return ((String[]) this.data[column])[this.blockLine];
            default: //BLOB, CLOB, BigDecimal, BigInteger, Time, Timestamp and Date
                return ((Object[]) this.data[column])[this.blockLine].toString();
        }
    }

    /**
     * Gets the current row value as a Java Object.
     *
     * @param column The column index starting from 0
     * @return The Object representation of the data type
     */
    public Object getValueAsObject(int column) {
        switch (this.jdbcSQLTypes[column]) {
            case Types.BOOLEAN:
                return ((byte[]) this.data[column])[this.blockLine] == 1;
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
