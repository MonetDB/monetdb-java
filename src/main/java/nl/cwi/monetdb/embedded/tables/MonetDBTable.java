package nl.cwi.monetdb.embedded.tables;

import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedException;
import nl.cwi.monetdb.embedded.mapping.AbstractColumn;
import nl.cwi.monetdb.embedded.mapping.AbstractResultTable;
import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedConnection;
import nl.cwi.monetdb.embedded.mapping.MonetDBRow;
import nl.cwi.monetdb.embedded.resultset.QueryResultSet;
import nl.cwi.monetdb.embedded.resultset.QueryResultSetColumn;

/**
 * Java representation of a MonetDB table. It's possible to perform several CRUD operations using the respective
 * provided interfaces.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class MonetDBTable extends AbstractResultTable {

    /**
     * The table's schema.
     */
    private final String schemaName;

    /**
     * The table's name.
     */
    private final String tableName;

    /**
     * The table's columns.
     */
    private final MonetDBTableColumn<?>[] columns;

    /**
     * The connection's C pointer.
     */
    private long connectionPointer;

    /**
     * These arrays are used for table imports.
     */
    private final int[] columnsJavaIndexes;
    private final int[] columnsMonetDBIndexes;
    private final String[] columnsNames;
    private final Class[] columnsClasses;

    public MonetDBTable(MonetDBEmbeddedConnection connection, long connectionPointer, String schemaName,
                        String tableName, MonetDBTableColumn<?>[] columns) {
        super(connection);
        this.connectionPointer = connectionPointer;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columns = columns;
        this.columnsJavaIndexes = new int[columns.length];
        this.columnsMonetDBIndexes = new int[columns.length];
        this.columnsNames = new String[columns.length];
        this.columnsClasses = new Class[columns.length];
        int i = 0;
        for(MonetDBTableColumn col : this.columns) {
            this.columnsJavaIndexes[i] = col.getMapping().ordinal();
            this.columnsMonetDBIndexes[i] = col.getInternalMonetDBTypeIndex();
            this.columnsNames[i] = col.getColumnName();
            this.columnsClasses[i] = col.getMapping().getJavaClass();
            i++;
        }
    }

    @Override
    protected void closeImplementation() { this.connectionPointer = 0; }

    @Override
    protected AbstractColumn<?>[] getColumns() { return this.columns; }

    @Override
    public int getNumberOfColumns() { return this.columns.length; }

    /**
     * Gets the current number of rows in the table, or -1 if an error in the database has ocurred.
     *
     * @return The number of rows in the table.
     */
    @Override
    public int getNumberOfRows() {
        int res;
        try {
            String query = "SELECT COUNT(*) FROM " + this.schemaName + "." + this.tableName + ";";
            QueryResultSet eqr = this.getConnection().sendQuery(query);
            QueryResultSetColumn<Long> eqc = eqr.getColumn(0);
            res = eqc.fetchFirstNColumnValues(1)[0].intValue();
            eqr.close();
        } catch (MonetDBEmbeddedException ex) {
            res = -1;
        }
        return res;
    }

    /**
     * Gets the table schema name.
     *
     * @return The table schema name
     */
    public String getSchemaName() { return schemaName; }

    /**
     * Gets the table name.
     *
     * @return The table name
     */
    public String getTableName() { return tableName; }

    /**
     * Gets the columns nullable indexes as an array.
     *
     * @return The columns nullable indexes as an array
     */
    public boolean[] getColumnNullableIndexes() {
        int i = 0;
        boolean[] result = new boolean[this.getNumberOfColumns()];
        for(MonetDBTableColumn col : this.columns) {
            result[i] = col.isNullable();
        }
        return result;
    }

    /**
     * Gets the columns default values in an array.
     *
     * @return The columns default values in an array
     */
    public String[] getColumnDefaultValues() {
        int i = 0;
        String[] result = new String[this.getNumberOfColumns()];
        for(MonetDBTableColumn col : this.columns) {
            result[i] = col.getDefaultValue();
        }
        return result;
    }

    /**
     * Private method to check the limits of iteration.
     *
     * @param iterator The iterator to check
     * @return An integer array with the limits fixed
     */
    private int[] checkIterator(IMonetDBTableBaseIterator iterator) {
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
        int[] limits = this.checkIterator(cursor);
        int res = 0, total = limits[1] - limits[0] + 1;
        String query = new StringBuffer("SELECT * FROM ").append(this.schemaName).append(".").append(this.tableName)
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
     * Iterate over the table using a {@link IMonetDBTableCursor}
     * instance asynchronously.
     *
     * @param iterator The iterator with the business logic
     * @return The number of rows iterated
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    /*public CompletableFuture<Integer> iterateTable(IMonetDBTableCursor iterator) throws MonetDBEmbeddedException {
        return CompletableFuture.supplyAsync(() -> this.iterateTable(iterator));
    }*/

    /**
     * Perform an update iteration over the table using a {@link nl.cwi.monetdb.embedded.tables.IMonetDBTableUpdater}
     * instance.
     *
     * @param updater The iterator with the business logic
     * @return The number of rows updated
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    /*public int updateRows(IMonetDBTableUpdater updater) throws MonetDBEmbeddedException {
        int[] limits = this.checkIterator(updater);
        RowUpdater ru = this.getRowUpdaterInternal(this.connectionPointer, this.schemaName, this.tableName,
                limits[0], limits[1]);
        while(ru.tryContinueIteration()) {
            updater.processNextRow(ru);
        }
        return ru.submitUpdates();
    }*/

    /**
     * Perform an update iteration over the table using a {@link nl.cwi.monetdb.embedded.tables.IMonetDBTableUpdater}
     * instance asynchronously.
     *
     * @param updater The iterator with the business logic
     * @return The number of rows updated
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    /*public CompletableFuture<Integer> updateRowsAsync(IMonetDBTableUpdater updater) throws MonetDBEmbeddedException {
        return CompletableFuture.supplyAsync(() -> this.updateRows(updater));
    }*/

    /**
     * Perform a removal iteration over the table using a {@link nl.cwi.monetdb.embedded.tables.IMonetDBTableRemover}
     * instance.
     *
     * @param remover The iterator with the business logic
     * @return The number of rows removed
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    /*public int removeRows(IMonetDBTableRemover remover) throws MonetDBEmbeddedException {
        int[] limits = this.checkIterator(remover);
        RowRemover rr = this.getRowRemoverInternal(this.connectionPointer, this.schemaName, this.tableName,
                limits[0], limits[1]);
        while(rr.tryContinueIteration()) {
            remover.processNextRow(rr);
        }
        return rr.submitDeletes();
    }*/

    /**
     * Perform a removal iteration over the table using a {@link nl.cwi.monetdb.embedded.tables.IMonetDBTableRemover}
     * instance asynchronously.
     *
     * @param remover The iterator with the business logic
     * @return The number of rows removed
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    /*public CompletableFuture<Integer> removeRowsAsync(IMonetDBTableRemover remover) throws MonetDBEmbeddedException {
        return CompletableFuture.supplyAsync(() -> this.removeRows(remover));
    }*/

    /**
     * Deletes all rows in the table.
     *
     * @return The number of rows removed
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    /*public int truncateTable() throws MonetDBEmbeddedException {
        return this.truncateTableInternal(this.connectionPointer, this.schemaName, this.tableName);
    }*/

    /**
     * Deletes all rows in the table asynchronously.
     *
     * @return The number of rows removed
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    /*public CompletableFuture<Integer> truncateTableAsync() throws MonetDBEmbeddedException {
        return CompletableFuture.supplyAsync(() -> this.truncateTable());
    }*/

    /**
     * Appends new rows to the table. As MonetDB's storage is column-wise, the method
     * {@link nl.cwi.monetdb.embedded.tables.MonetDBTable#appendColumns(Object[][]) appendColumns} is preferable
     * over this one.
     *
     * @param rows An array of rows to append
     * @return The number of rows appended
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public int appendRows(Object[][] rows) throws MonetDBEmbeddedException {
        int numberOfRows = rows.length, numberOfColumns = this.getNumberOfColumns();
        if(numberOfRows == 0) {
            throw new ArrayStoreException("Appending 0 rows?");
        }
        Object[][] transposed = new Object[numberOfColumns][numberOfRows];

        for (int i = 0; i < numberOfRows; i++) {
            if(rows[i].length != numberOfColumns) {
                throw new ArrayStoreException("The values array at row " + i + " differs from the number of columns!");
            }
            for (int j = 0; j < numberOfColumns; j++) {
                transposed[j][i] = rows[i][j];
            }
        }
        return this.appendColumns(transposed);
    }

    /**
     * Appends new rows to the table asynchronously.
     *
     * @param rows An array of rows to append
     * @return The number of rows appended
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    /*public CompletableFuture<Integer> appendRowsAsync(Object[][] rows) throws MonetDBEmbeddedException {
        return CompletableFuture.supplyAsync(() -> this.appendRows(schemaName, tableName));
    }*/

    /**
     * Appends new rows to the table column-wise. As MonetDB's storage is column-wise, this method is preferable over
     * {@link nl.cwi.monetdb.embedded.tables.MonetDBTable#appendRows(Object[][]) appendRows} method.
     *
     * @param columns An array of columns to append
     * @return The number of rows appended
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public int appendColumns(Object[][] columns) throws MonetDBEmbeddedException {
        int numberOfRows = columns[0].length, numberOfColumns = this.getNumberOfColumns();
        if (columns.length != numberOfColumns) {
            throw new ArrayStoreException("The number of columns differs from the table's number of columns!");
        }
        if(numberOfRows == 0) {
            throw new ArrayStoreException("Appending 0 rows?");
        }
        for (int i = 0; i < numberOfColumns; i++) {
            if(columns[i].length != numberOfRows) {
                throw new ArrayStoreException("The number of rows in each column is not consistent!");
            }
        }
        return this.appendColumnsInternal(this.connectionPointer, this.schemaName, this.tableName, this.columnsJavaIndexes,
                this.columnsMonetDBIndexes,  this.columnsNames, this.columnsClasses, columns);
    }

    /**
     * Appends new rows to the table column-wise and asynchronously.
     *
     * @param columns An array of columns to append
     * @return The number of rows appended
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    /*public CompletableFuture<Integer> appendColumnsAsync(Object[][] columns) throws MonetDBEmbeddedException {
        return CompletableFuture.supplyAsync(() -> this.appendColumns(schemaName, tableName));
    }*/

    /**
     * Internal implementation of table truncation.
     */
    /*private native int truncateTableInternal(long connectionPointer, String schemaName, String tableName)
            throws MonetDBEmbeddedException;*/

    /**
     * Internal implementation of columns insertion.
     */
    private native int appendColumnsInternal(long connectionPointer, String schemaName, String tableName,
                                             int[] javaindexes, int[] monetDBindexes, String[] columnsNames,
                                             Class[] classes, Object[][] columns)
            throws MonetDBEmbeddedException;
}
