/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded;

/**
 * The result set from a sendQuery method from a connection.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public abstract class AbstractQueryResultSet extends AbstractStatementResult implements Iterable {

    /**
     * The number of columns in the query result.
     */
    protected final int numberOfColumns;

    /**
     * The number of rows in the query result.
     */
    protected final int numberOfRows;

    protected AbstractQueryResultSet(MonetDBEmbeddedConnection connection, long resultPointer, int numberOfColumns,
                                     int numberOfRows) {
        super(connection, resultPointer);
        this.numberOfColumns = numberOfColumns;
        this.numberOfRows = numberOfRows;
    }

    /**
     * Get the query set column values as an Iterable.
     *
     * @return An Iterable over the columns
     */
    protected abstract Iterable<AbstractColumn<?>> getIterable();

    /**
     * Returns the number of columns in the result set.
     *
     * @return Number of columns
     */
    public int getNumberOfColumns() {
        return this.numberOfColumns;
    }

    /**
     * Returns the number of rows in the result set.
     *
     * @return Number of rows
     */
    public int getNumberOfRows() {
        return this.numberOfRows;
    }

    /**
     * Get the columns names as a string array.
     *
     * @return The columns names array
     */
    public String[] getColumnNames() {
        int i = 0;
        String[] result = new String[this.numberOfColumns];
        for(AbstractColumn col : this.getIterable()) {
            result[i] = col.getColumnName();
        }
        return result;
    }

    /**
     * Get the columns types as a string array.
     *
     * @return The columns types array
     */
    public String[] getColumnTypes() {
        int i = 0;
        String[] result = new String[this.numberOfColumns];
        for(AbstractColumn col : this.getIterable()) {
            result[i] = col.getColumnType();
        }
        return result;
    }

    /**
     * Get the Java mappings as a MonetDBToJavaMapping array.
     *
     * @return The columns MonetDBToJavaMapping array
     */
    public MonetDBToJavaMapping[] getMappings() {
        int i = 0;
        MonetDBToJavaMapping[] result = new MonetDBToJavaMapping[this.numberOfColumns];
        for(AbstractColumn col : this.getIterable()) {
            result[i] = col.getMapping();
        }
        return result;
    }

    /**
     * Get the columns digits as a int array.
     *
     * @return The columns digits array
     */
    public int[] getColumnDigits() {
        int i = 0;
        int[] result = new int[this.numberOfColumns];
        for(AbstractColumn col : this.getIterable()) {
            result[i] = col.getColumnDigits();
        }
        return result;
    }

    /**
     * Get the columns scales as a int array.
     *
     * @return The columns scales array
     */
    public int[] getColumnScales() {
        int i = 0;
        int[] result = new int[this.numberOfColumns];
        for(AbstractColumn col : this.getIterable()) {
            result[i] = col.getColumnScale();
        }
        return result;
    }

    /**
     * Get a columns' values from the result set by index.
     *
     * @param index QueryResultSetColumn index (starting from 0)
     * @return The columns, {@code null} if index not in bounds
     */
    public abstract <T> QueryResultSetColumn<T> getColumn(int index);

    /**
     * Get a columns from the result set by name.
     *
     * @param name QueryResultSetColumn name
     * @return The columns
     */
    public <T> QueryResultSetColumn<T> getColumn(String name) {
        int index = 0;
        for (AbstractColumn col : this.getIterable()) {
            if (col.getColumnName().equals(name)) {
                return this.getColumn(index);
            }
            index++;
        }
        throw new ArrayIndexOutOfBoundsException("The columns is not present in the result set!");
    }

}
