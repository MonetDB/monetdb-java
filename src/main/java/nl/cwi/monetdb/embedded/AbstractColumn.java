/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded;

/**
 * A single Java representation of a MonetDB column.
 *
 * @param <T> A Java class mapped to a MonetDB data type
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public abstract class AbstractColumn<T> implements Iterable<T> {

    /**
     * Index on the result set.
     */
    protected final int resultSetIndex;

    /**
     * The number of rows in this column.
     */
    protected final int numberOfRows;

    /**
     * The name of the columns in the query result.
     */
    protected final String columnName;

    /**
     * The Mapping between MonetDB type and the Java Class.
     */
    protected final MonetDBToJavaMapping mapping;

    /**
     * The number of digits (radix 2) for numeric types or max length for character/binary strings.
     */
    protected final int columnDigits;

    /**
     * 	The precision after decimal point. Only applicable for decimal/numeric types.
     */
    protected final int columnScale;

    protected AbstractColumn(int resultSetIndex, int numberOfRows, String columnName, String columnType,
                             int columnDigits, int columnScale) {
        this.resultSetIndex = resultSetIndex;
        this.numberOfRows = numberOfRows;
        this.columnName = columnName;
        this.mapping = MonetDBToJavaMapping.GetJavaMappingFromMonetDBString(columnType);
        this.columnDigits = columnDigits;
        this.columnScale = columnScale;
    }

    /**
     * Get the number of rows in this column.
     *
     * @return The number of rows
     */
    public int getNumberOfRows() { return numberOfRows; }

    /**
     * Get the result set index of the column.
     *
     * @return The index number
     */
    public int getResultSetIndex() { return resultSetIndex; }

    /**
     * Get the name of the column.
     *
     * @return The column name
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * Get the type of the column.
     *
     * @return The Column type
     */
    public String getColumnType() { return mapping.toString(); }

    /**
     * Get the Java mapping of the column.
     *
     * @return A enum constant of the Java mapping
     */
    public MonetDBToJavaMapping getMapping() { return mapping; }

    /**
     * Get column digits of the column.
     *
     * @return The number of digits
     */
    public int getColumnDigits() { return columnDigits; }

    /**
     * Get scale of the column.
     *
     * @return The scale
     */
    public int getColumnScale() { return columnScale; }

}
