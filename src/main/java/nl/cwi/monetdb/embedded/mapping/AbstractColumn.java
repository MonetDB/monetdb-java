/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded.mapping;

/**
 * A single Java representation of a MonetDB column.
 *
 * @param <T> A Java class mapped to a MonetDB data type
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public abstract class AbstractColumn<T> {

    /**
     * The column index on the result set.
     */
    protected final int resultSetIndex;

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
     * The precision after decimal point. Only applicable for decimal/numeric types.
     */
    protected final int columnScale;

    protected AbstractColumn(int resultSetIndex, String columnName, String columnType, int columnDigits,
                             int columnScale) {
        this.resultSetIndex = resultSetIndex;
        this.columnName = columnName;
        this.mapping = MonetDBToJavaMapping.GetJavaMappingFromMonetDBString(columnType);
        this.columnDigits = columnDigits;
        this.columnScale = columnScale;
    }

    /**
     * Gets the result set index of the column.
     *
     * @return The index number
     */
    public int getResultSetIndex() { return resultSetIndex; }

    /**
     * Gets the name of the column.
     *
     * @return The column name
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * Gets the type of the column.
     *
     * @return The Column type
     */
    public String getColumnType() { return mapping.toString(); }

    /**
     * Gets the Java mapping of the column.
     *
     * @return A enum constant of the Java mapping
     */
    public MonetDBToJavaMapping getMapping() { return mapping; }

    /**
     * Gets the number digits of the column.
     *
     * @return The number of digits
     */
    public int getColumnDigits() { return columnDigits; }

    /**
     * Gets the scale of the column.
     *
     * @return The scale
     */
    public int getColumnScale() { return columnScale; }
}
