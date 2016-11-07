package nl.cwi.monetdb.embedded.tables;

import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedException;
import nl.cwi.monetdb.embedded.mapping.AbstractColumn;
import nl.cwi.monetdb.embedded.mapping.AbstractResultTable;
import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedConnection;

/**
 * Java representation of a MonetDB table.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class MonetDBTable extends AbstractResultTable {

    /**
     * The table schema
     */
    private final String tableSchema;

    /**
     * The table name
     */
    private final String tableName;

    /**
     * The table columns
     */
    private final MonetDBTableColumn<?>[] columns;

    public MonetDBTable(MonetDBEmbeddedConnection connection, String tableSchema, String tableName,
                        MonetDBTableColumn<?>[] columns) {
        super(connection);
        this.tableSchema = tableSchema;
        this.tableName = tableName;
        this.columns = columns;
    }

    /**
     * Let's see...
     */
    @Override
    protected void closeImplementation() {}

    @Override
    protected AbstractColumn<?>[] getColumns() { return columns; }

    @Override
    public int getNumberOfColumns() { return columns.length; }

    @Override
    public int getNumberOfRows() {
        return 0;
    }

    /**
     * Get the table schema name
     *
     * @return The table schema name
     */
    public String getTableSchema() { return tableSchema; }

    /**
     * Get the table name
     *
     * @return The table name
     */
    public String getTableName() { return tableName; }

    public void iterateTable(IMonetDBTableIterator iterator) throws MonetDBEmbeddedException {
        RowIterator ri = new RowIterator(this, iterator.getFirstRowToIterate(), iterator.getLastRowToIterate());
        while(this.getNextRow(ri)) {
            iterator.nextRow(ri);
        }
    }

    public int updateRows(IMonetDBTableUpdater updater) throws MonetDBEmbeddedException {
        int res = 0;
        RowUpdater ru = new RowUpdater(this, updater.getFirstRowToIterate(), updater.getLastRowToIterate());
        while(this.getNextRow(ru)) {
            updater.updateNextRow(ru);
            if(ru.toUpdate()) {
                res++;
                this.updateNextRow(ru);
            }

        }
        return res;
    }

    public int removeRows(IMonetDBTableRemover remover) throws MonetDBEmbeddedException {
        int res = 0;
        RowRemover rr = new RowRemover(this, remover.getFirstRowToIterate(), remover.getLastRowToIterate());
        while(this.getNextRow(rr)) {
            remover.removeNextRow(rr);
            if(rr.isToRemove()) {
                res++;
                this.removeNextRow(rr);
            }
        }
        return res;
    }

    public native int truncate();

    public int appendRows(Object[][] rows) {
        return 0;
    }

    private native boolean getNextRow(RowIterator ri) throws MonetDBEmbeddedException;

    private native boolean updateNextRow(RowUpdater ri) throws MonetDBEmbeddedException;

    private native boolean removeNextRow(RowRemover rr) throws MonetDBEmbeddedException;
}
