package nl.cwi.monetdb.embedded.tables;

import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedException;
import nl.cwi.monetdb.embedded.mapping.AbstractColumn;
import nl.cwi.monetdb.embedded.mapping.AbstractResultTable;
import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedConnection;
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

    public MonetDBTable(MonetDBEmbeddedConnection connection, long connectionPointer, String schemaName,
                        String tableName, MonetDBTableColumn<?>[] columns) {
        super(connection);
        this.connectionPointer = connectionPointer;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columns = columns;
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
        int res = -1;
        try {
            String query = "SELECT COUNT(*) FROM " + this.schemaName + "." + this.tableName + ";";
            QueryResultSet eqr = this.getConnection().sendQuery(query);
            QueryResultSetColumn<Integer> eqc = eqr.getColumn(0);
            res = eqc.fetchFirstNColumnValues(1)[0];
        } catch (MonetDBEmbeddedException ex) {
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
     * Iterate over the table using a {@link nl.cwi.monetdb.embedded.tables.IMonetDBTableIterator} instance.
     *
     * @param iterator The iterator with the business logic
     * @return The number of rows iterated
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public int iterateTable(IMonetDBTableIterator iterator) throws MonetDBEmbeddedException {
        int res = 0;
        RowIterator ri = this.getRowIteratorInternal(this.connectionPointer, this.schemaName, this.tableName,
                iterator.getFirstRowToIterate(), iterator.getLastRowToIterate());
        while(ri.tryContinueIteration()) {
            iterator.processNextRow(ri);
            res++;
        }
        return res;
    }

    /**
     * Iterate over the table using a {@link nl.cwi.monetdb.embedded.tables.IMonetDBTableIterator}
     * instance asynchronously.
     *
     * @param iterator The iterator with the business logic
     * @return The number of rows iterated
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    /*public CompletableFuture<Integer> iterateTable(IMonetDBTableIterator iterator) throws MonetDBEmbeddedException {
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
    public int updateRows(IMonetDBTableUpdater updater) throws MonetDBEmbeddedException {
        RowUpdater ru = this.getRowUpdaterInternal(this.connectionPointer, this.schemaName, this.tableName,
                updater.getFirstRowToIterate(), updater.getLastRowToIterate());
        while(ru.tryContinueIteration()) {
            updater.processNextRow(ru);
        }
        return ru.submitUpdates();
    }

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
    public int removeRows(IMonetDBTableRemover remover) throws MonetDBEmbeddedException {
        RowRemover rr = this.getRowRemoverInternal(this.connectionPointer, this.schemaName, this.tableName,
                remover.getFirstRowToIterate(), remover.getLastRowToIterate());
        while(rr.tryContinueIteration()) {
            remover.processNextRow(rr);
        }
        return rr.submitDeletes();
    }

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
    public int truncateTable() throws MonetDBEmbeddedException {
        return this.truncateTableInternal(this.connectionPointer, this.schemaName, this.tableName);
    }

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
     * Appends new rows to the table.
     *
     * @param rows An array of rows to append
     * @return The number of rows appended
     * @throws MonetDBEmbeddedException If an error in the database occurred
     */
    public int appendRows(Object[][] rows) throws MonetDBEmbeddedException {
        int i = 0;
        for (Object[] row : rows) {
            if (row.length != this.getNumberOfColumns()) {
                throw new ArrayStoreException("The values array at row " + i + " differs from the number of columns!");
            }
            i++;
        }
        return this.appendRowsInternal(this.connectionPointer, this.schemaName, this.tableName, rows);
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
     * Internal implementation to get a table iterator.
     */
    private native RowIterator getRowIteratorInternal(long connectionPointer, String schemaName, String tableName,
                                                     int firstRowToIterate, int lastRowToIterate) throws MonetDBEmbeddedException;

    /**
     * Internal implementation to get a table updater iterator.
     */
    private native RowUpdater getRowUpdaterInternal(long connectionPointer, String schemaName, String tableName,
                                                      int firstRowToIterate, int lastRowToIterate) throws MonetDBEmbeddedException;

    /**
     * Internal implementation to get a table remover iterator.
     */
    private native RowRemover getRowRemoverInternal(long connectionPointer, String schemaName, String tableName,
                                                    int firstRowToIterate, int lastRowToIterate) throws MonetDBEmbeddedException;

    /**
     * Internal implementation of table truncation.
     */
    private native int truncateTableInternal(long connectionPointer, String schemaName, String tableName)
            throws MonetDBEmbeddedException;

    /**
     * Internal implementation of rows insertion.
     */
    private native int appendRowsInternal(long connectionPointer, String schemaName, String tableName, Object[][] rows)
            throws MonetDBEmbeddedException;
}
