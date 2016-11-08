package nl.cwi.monetdb.embedded.tables;

import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedException;
import nl.cwi.monetdb.embedded.mapping.AbstractColumn;
import nl.cwi.monetdb.embedded.mapping.AbstractResultTable;
import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedConnection;
import nl.cwi.monetdb.embedded.resultset.QueryResultSet;
import nl.cwi.monetdb.embedded.resultset.QueryResultSetColumn;
import nl.cwi.monetdb.embedded.utils.StringEscaper;

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

    public MonetDBTable(MonetDBEmbeddedConnection connection, String schemaName, String tableName,
                        MonetDBTableColumn<?>[] columns) {
        super(connection);
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columns = columns;
    }

    @Override
    protected void closeImplementation() {}

    @Override
    protected AbstractColumn<?>[] getColumns() { return columns; }

    @Override
    public int getNumberOfColumns() { return columns.length; }

    @Override
    public int getNumberOfRows() {
        int res = -1;
        try {
            String qschemaName = StringEscaper.SQLStringEscape(this.schemaName);
            String qtableName = StringEscaper.SQLStringEscape(this.tableName);
            String query = "SELECT COUNT(*) FROM " + qschemaName + "." + qtableName + ";";
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
     */
    public int iterateTable(IMonetDBTableIterator iterator) {
        int res = 0;
        RowIterator ri = new RowIterator(this, iterator.getFirstRowToIterate(), iterator.getLastRowToIterate());
        while(ri.getNextTableRow()) {
            iterator.nextRow(ri);
            res++;
        }
        return res;
    }

    /**
     * Perform an update iteration over the table using a {@link nl.cwi.monetdb.embedded.tables.IMonetDBTableUpdater}
     * instance.
     *
     * @param updater The iterator with the business logic
     * @return The number of rows updated
     */
    public int updateRows(IMonetDBTableUpdater updater) {
        int res = 0;
        RowUpdater ru = new RowUpdater(this, updater.getFirstRowToIterate(), updater.getLastRowToIterate());
        while(ru.getNextTableRow()) {
            updater.nextRow(ru);
            if(ru.tryUpdate()) {
                res++;
            }
        }
        return res;
    }

    /**
     * Perform a removal iteration over the table using a {@link nl.cwi.monetdb.embedded.tables.IMonetDBTableRemover}
     * instance.
     *
     * @param remover The iterator with the business logic
     * @return The number of rows removed
     */
    public int removeRows(IMonetDBTableRemover remover) {
        int res = 0;
        RowRemover rr = new RowRemover(this, remover.getFirstRowToIterate(), remover.getLastRowToIterate());
        while(rr.getNextTableRow()) {
            remover.nextRow(rr);
            if(rr.tryRemove()) {
                res++;
            }
        }
        return res;
    }

    /**
     * Deletes all rows in the table.
     *
     * @return The number of rows removed
     */
    public native int truncateTable();

    /**
     * Appends new rows to the table.
     *
     * @param rows An array of rows to append
     * @return The number of rows appended
     */
    public int appendRows(Object[][] rows) {
        int i = 0;
        for (Object[] row : rows) {
            if (row.length != this.getNumberOfColumns()) {
                throw new ArrayStoreException("The values array at row " + i + " differs from the number of columns!");
            }
            i++;
        }
        return this.appendRowsInternal(rows);
    }

    /**
     * Internal implementation of rows insertion.
     */
    private native int appendRowsInternal(Object[][] rows);
}
