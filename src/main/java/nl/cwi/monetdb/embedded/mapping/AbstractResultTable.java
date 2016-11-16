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
    public abstract String[] getColumnNames();

    /**
     * Gets the columns types as a string array.
     *
     * @return The columns types array
     */
    public abstract String[] getColumnTypes();

    /**
     * Gets the Java mappings as a MonetDBToJavaMapping array.
     *
     * @return The columns MonetDBToJavaMapping array
     */
    public abstract MonetDBToJavaMapping[] getMappings();

    /**
     * Gets the columns digits as an integer array.
     *
     * @return The columns digits array
     */
    public abstract int[] getColumnDigits();

    /**
     * Gets the columns scales as an integer array.
     *
     * @return The columns scales array
     */
    public abstract int[] getColumnScales();
}
