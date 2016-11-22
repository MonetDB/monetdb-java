/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded.tables;

import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedException;
import nl.cwi.monetdb.embedded.mapping.AbstractResultTable;
import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedConnection;
import nl.cwi.monetdb.embedded.mapping.MonetDBRow;
import nl.cwi.monetdb.embedded.mapping.MonetDBToJavaMapping;
import nl.cwi.monetdb.embedded.resultset.QueryResultSet;
import nl.cwi.monetdb.embedded.resultset.QueryResultSetLongColumn;

/**
 * Java representation of a MonetDB table. It's possible to perform several CRUD operations using the respective
 * provided interfaces.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class MonetDBTable extends AbstractResultTable {

    private final String tableSchema;

    private final String tableName;

    protected MonetDBTable(MonetDBEmbeddedConnection connection, String tableSchema, String tableName) {
        super(connection);
        this.tableSchema = tableSchema;
        this.tableName = tableName;
    }

    @Override
    public native int getNumberOfColumns();

    /**
     * Gets the current number of rows in the table, or -1 if an error in the database has occurred.
     *
     * @return The number of rows in the table.
     */
    @Override
    public int getNumberOfRows() {
        int res;
        try {
            String query = "SELECT COUNT(*) FROM " + this.getTableSchema() + "." + this.getTableName() + ";";
            QueryResultSet eqr = this.getConnection().sendQuery(query);
            QueryResultSetLongColumn eqc = eqr.getLongColumnByIndex(0);
            res = (int) eqc.fetchFirstNColumnValues(1)[0];
            eqr.close();
        } catch (MonetDBEmbeddedException ex) {
            res = -1;
        }
        return res;
    }

    @Override
    public native String[] getColumnNames();

    @Override
    public native String[] getColumnTypes();

    @Override
    public native MonetDBToJavaMapping[] getMappings();

    @Override
    public native int[] getColumnDigits();

    @Override
    public native int[] getColumnScales();

    /**
     * Gets the table schema name.
     *
     * @return The table schema name
     */
    public String getTableSchema() { return this.tableSchema; }

    /**
     * Gets the table name.
     *
     * @return The table name
     */
    public String getTableName() { return this.tableName; }

    /**
     * Gets the columns nullable indexes as an array.
     *
     * @return The columns nullable indexes as an array
     */
    public native boolean[] getColumnNullableIndexes();

    /**
     * Gets the columns default values in an array.
     *
     * @return The columns default values in an array
     */
    public native String[] getColumnDefaultValues();

    /**
     * Gets a column metadata by index.
     *
     * @param index The column index (starting from 0)
     * @return The column metadata, {@code null} if index not in bounds
     */
    public native MonetDBTableColumn getColumnMetadataByIndex(int index);

    /**
     * Gets a column metadata by name.
     *
     * @param name The column name
     * @return The column metadata, {@code null} if not found
     */
    public native MonetDBTableColumn getColumnMetadataByName(String name);

    /**
     * Gets all columns metadata.
     *
     * @return An array instance of columns metadata
     */
    public native MonetDBTableColumn[] getAllColumnsMetadata();

    /**
     * Private method to check the limits of iteration.
     *
     * @param iterator The iterator to check
     * @return An integer array with the limits fixed
     */
    private int[] prepareIterator(IMonetDBTableBaseIterator iterator) {
        int[] res = {iterator.getFirstRowToIterate(), iterator.getLastRowToIterate()};
        if(res[1] < res[0]) {
            int aux = res[0];
            res[0] = res[1];
            res[0] = aux;
        }
        if (res[0] < 1) {
            res[0] = 1;
        }
        int numberOfRows = this.getNumberOfRows();
        if (res[1] >= numberOfRows) {
            res[1] = numberOfRows;
        }
        return res;
    }

    /**
     * Iterate over the table using a {@link IMonetDBTableCursor} instance.
     *
     * @param cursor The iterator with the business logic
     * @return The number of rows iterated
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public int iterateTable(IMonetDBTableCursor cursor) throws MonetDBEmbeddedException {
        int[] limits = this.prepareIterator(cursor);
        int res = 0, total = limits[1] - limits[0] + 1;
        String query = new StringBuffer("SELECT * FROM ").append(this.getTableSchema()).append(".").append(this.getTableName())
                .append(" LIMIT ").append(total).append(" OFFSET ").append(limits[0] - 1).append(";").toString();

        QueryResultSet eqr = this.getConnection().sendQuery(query);
        MonetDBRow[] array = eqr.fetchAllRowValues().getAllRows();
        eqr.close();
        Object[][] data = new Object[eqr.getNumberOfRows()][this.getNumberOfColumns()];
        for(int i = 0 ; i < eqr.getNumberOfRows() ; i++) {
            data[i] = array[i].getAllColumns();
        }

        RowIterator ri = new RowIterator(this, data, limits[0], limits[1]);
        while(ri.tryContinueIteration()) {
            cursor.processNextRow(ri);
            res++;
        }
        return res;
    }

    /**
     * Appends new rows to the table column-wise.
     *
     * @param data An array of columns to append
     * @return The number of rows appended
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public int appendColumns(Object[] data) throws MonetDBEmbeddedException {
        MonetDBToJavaMapping[] mappings = this.getMappings();
        if (data.length != mappings.length) {
            throw new ArrayStoreException("The number of columns between the data nad the classes is not consistent!");
        }
        if (mappings.length != this.getNumberOfColumns()) {
            throw new ArrayStoreException("The number of columns between data and the table is not consistent!");
        }
        int[] javaIndexes = new int[mappings.length];
        Class<?>[] javaClasses = new Class<?>[mappings.length];
        for (int i = 0; i < mappings.length; i++) {
            javaIndexes[i] = mappings[i].ordinal();
            javaClasses[i] = mappings[i].getJavaClass();
        }
        return this.appendColumnsInternal(data, javaIndexes, javaClasses);
    }

    /*
     * Appends new rows to the table column-wise and asynchronously.
     *
     * @param data An array of columns to append
     * @return The number of rows appended
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    /*public CompletableFuture<Integer> appendColumnsAsync(Object[] data) throws MonetDBEmbeddedException {
        return CompletableFuture.supplyAsync(() -> this.appendColumns(data));
    }*/

    @Override
    protected void closeImplementation() {}

    private native int appendColumnsInternal(Object[] data, int[] javaIndexes, Class<?>[] javaClasses)
            throws MonetDBEmbeddedException;
}
