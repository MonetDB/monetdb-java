package nl.cwi.monetdb.embedded.mapping;

import nl.cwi.monetdb.embedded.env.AbstractConnectionResult;
import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedConnection;

/**
 * Base class for statement results with data
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public abstract class AbstractResultTable extends AbstractConnectionResult {

    public AbstractResultTable(MonetDBEmbeddedConnection connection) { super(connection); }

    /**
     * Returns an array of columns in the result set.
     *
     * @return An array of columns in the result set
     */
    protected abstract AbstractColumn<?>[] getColumns();

    /**
     * Returns the number of columns in the result set.
     *
     * @return Number of columns
     */
    public abstract int getNumberOfColumns();

    /**
     * Returns the number of rows in the result set.
     *
     * @return Number of rows
     */
    public abstract int getNumberOfRows();

    /**
     * Gets the columns names as a string array.
     *
     * @return The columns names array
     */
    public String[] getColumnNames() {
        int i = 0;
        String[] result = new String[this.getNumberOfColumns()];
        for(AbstractColumn col : this.getColumns()) {
            result[i] = col.getColumnName();
        }
        return result;
    }

    /**
     * Gets the columns types as a string array.
     *
     * @return The columns types array
     */
    public String[] getColumnTypes() {
        int i = 0;
        String[] result = new String[this.getNumberOfColumns()];
        for(AbstractColumn col : this.getColumns()) {
            result[i] = col.getColumnType();
        }
        return result;
    }

    /**
     * Gets the Java mappings as a MonetDBToJavaMapping array.
     *
     * @return The columns MonetDBToJavaMapping array
     */
    public MonetDBToJavaMapping[] getMappings() {
        int i = 0;
        MonetDBToJavaMapping[] result = new MonetDBToJavaMapping[this.getNumberOfColumns()];
        for(AbstractColumn col : this.getColumns()) {
            result[i] = col.getMapping();
        }
        return result;
    }

    /**
     * Gets the columns digits as an integer array.
     *
     * @return The columns digits array
     */
    public int[] getColumnDigits() {
        int i = 0;
        int[] result = new int[this.getNumberOfColumns()];
        for(AbstractColumn col : this.getColumns()) {
            result[i] = col.getColumnDigits();
        }
        return result;
    }

    /**
     * Gets the columns scales as an integer array.
     *
     * @return The columns scales array
     */
    public int[] getColumnScales() {
        int i = 0;
        int[] result = new int[this.getNumberOfColumns()];
        for(AbstractColumn col :this.getColumns()) {
            result[i] = col.getColumnScale();
        }
        return result;
    }
}
