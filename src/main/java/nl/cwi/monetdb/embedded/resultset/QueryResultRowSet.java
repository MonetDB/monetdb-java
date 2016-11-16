/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded.resultset;

import nl.cwi.monetdb.embedded.mapping.AbstractRowSet;
import nl.cwi.monetdb.embedded.mapping.MonetDBRow;
import nl.cwi.monetdb.embedded.mapping.MonetDBToJavaMapping;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.ListIterator;

/**
 * The row result set from a sendQuery.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class QueryResultRowSet extends AbstractRowSet implements Iterable {

    /**
     * The original query result set this row set belongs.
     */
    private final QueryResultSet queryResultSet;

    protected QueryResultRowSet(MonetDBToJavaMapping[] mappings, Object[][] rows, QueryResultSet queryResultSet) {
        super(mappings, rows);
        this.queryResultSet = queryResultSet;
    }

    /**
     * Gets the original query result set this row set belongs.
     *
     * @return The original query result set this row set belongs
     */
    public QueryResultSet getQueryResultSet() { return queryResultSet; }

    /**
     * Gets all rows of this set.
     *
     * @return All rows of this set
     */
    public MonetDBRow[] getAllRows() { return rows; }

    /**
     * Gets the number of rows in this set.
     *
     * @return The number of rows in this set
     */
    public int getNumberOfRows() { return rows.length; }

    /**
     * Gets a single row in this set.
     *
     * @param row The index of the row to retrieve
     * @return A single row in this set
     */
    public MonetDBRow getSingleRow(int row) { return rows[row]; }

    /**
     * Gets a single value in this set as a Java class.
     *
     * @param <T> A Java class mapped to a MonetDB data type
     * @param row The index of the row to retrieve
     * @param column The index of the column to retrieve
     * @param javaClass The Java class to map
     * @return The value mapped to a instance of the provided class
     */
    public <T> T getSingleValueByIndex(int row, int column, Class<T> javaClass) {
        return javaClass.cast(this.rows[row].getColumnByIndex(column));
    }

    /**
     * Gets a single value in this set as a Java class using the default mapping.
     *
     * @param <T> A Java class mapped to a MonetDB data type
     * @param row The index of the row to retrieve
     * @param column The index of the column to retrieve
     * @return The value mapped to a instance of the provided class
     */
    public <T> T getSingleValueByIndex(int row, int column) {
        Class<T> javaClass = this.mappings[column].getJavaClass();
        return javaClass.cast(this.rows[row].getColumnByIndex(column));
    }

    /**
     * Gets a single value in this set as a Java class.
     *
     * @param <T> A Java class mapped to a MonetDB data type
     * @param row The index of the row to retrieve
     * @param columnName The name of the column to retrieve
     * @param javaClass The Java class to map
     * @return The value mapped to a instance of the provided class
     */
    public <T> T getSingleValueByName(int row, String columnName, Class<T> javaClass) {
        String[] colNames = this.getQueryResultSet().getColumnNames();
        int index = 0;
        for (String colName : colNames) {
            if (columnName.equals(colName)) {
                return this.getSingleValueByIndex(row, index, javaClass);
            }
            index++;
        }
        throw new ArrayIndexOutOfBoundsException("The column is not present in the result set!");
    }

    /**
     * Gets a single value in this set as a Java class using the default mapping.
     *
     * @param <T> A Java class mapped to a MonetDB data type
     * @param row The index of the row to retrieve
     * @param columnName The name of the column to retrieve
     * @return The value mapped to a instance of the provided class
     */
    public <T> T getSingleValueByName(int row, String columnName) {
        String[] colNames = this.getQueryResultSet().getColumnNames();
        int index = 0;
        for (String colName : colNames) {
            if (columnName.equals(colName)) {
                return this.getSingleValueByIndex(row, index);
            }
            index++;
        }
        throw new ArrayIndexOutOfBoundsException("The column is not present in the result set!");
    }

    /**
     * Gets a column in this set as a Java class.
     *
     * @param <T> A Java class mapped to a MonetDB data type
     * @param column The index of the column to retrieve
     * @param javaClass The Java class
     * @return The value mapped to a instance of the provided class
     */
    @SuppressWarnings("unchecked")
    public <T> T[] getColumnByIndex(int column, Class<T> javaClass) {
        T[] res = (T[]) Array.newInstance(javaClass, this.rows.length);
        for(int i = 0 ; i < this.rows.length ; i++) {
            res[i] = this.rows[i].getColumnByIndex(column);
        }
        return res;
    }

    /**
     * Gets a column in this set as a Java class using the default mapping.
     *
     * @param <T> A Java class mapped to a MonetDB data type
     * @param column The index of the column to retrieve
     * @return The value mapped to a instance of the provided class
     */
    @SuppressWarnings("unchecked")
    public <T> T[] getColumnByIndex(int column) {
        T[] res = (T[]) Array.newInstance(this.mappings[column].getJavaClass(), this.rows.length);
        for(int i = 0 ; i < this.rows.length ; i++) {
            res[i] = this.rows[i].getColumnByIndex(column);
        }
        return res;
    }

    /**
     * Gets a column in this set as a Java class.
     *
     * @param <T> A Java class mapped to a MonetDB data type
     * @param name The name of the column to retrieve
     * @param javaClass The Java class
     * @return The value mapped to a instance of the provided class
     */
    public <T> T[] getColumnByName(String name, Class<T> javaClass) {
        String[] colNames = this.getQueryResultSet().getColumnNames();
        int index = 0;
        for (String colName : colNames) {
            if (name.equals(colName)) {
                return this.getColumnByIndex(index, javaClass);
            }
            index++;
        }
        throw new ArrayIndexOutOfBoundsException("The column is not present in the result set!");
    }

    /**
     * Gets a column in this set as a Java class using the default mapping.
     *
     * @param <T> A Java class mapped to a MonetDB data type
     * @param name The name of the column to retrieve
     * @return The value mapped to a instance of the provided class
     */
    public <T> T[] getColumnByName(String name) {
        String[] colNames = this.getQueryResultSet().getColumnNames();
        int index = 0;
        for (String colName : colNames) {
            if (name.equals(colName)) {
                return this.getColumnByIndex(index);
            }
            index++;
        }
        throw new ArrayIndexOutOfBoundsException("The column is not present in the result set!");
    }

    @Override
    public ListIterator<MonetDBRow> iterator() { return Arrays.asList(this.rows).listIterator(); }
}
