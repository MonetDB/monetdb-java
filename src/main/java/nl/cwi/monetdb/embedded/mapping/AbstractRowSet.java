package nl.cwi.monetdb.embedded.mapping;

/**
 * A row set retrieved from an embedded MonetDB query result. All the values in this set are already mapped
 * to Java classes a priori.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public abstract class AbstractRowSet {

    /**
     * The MonetDB-To-Java mappings of the columns.
     */
    protected final MonetDBToJavaMapping[] mappings;

    /**
     * The rows of this set.
     */
    protected final MonetDBRow[] rows;

    protected AbstractRowSet(MonetDBToJavaMapping[] mappings, Object[][] rows) {
        this.mappings = mappings;
        this.rows = new MonetDBRow[rows.length];
        for(int i = 0 ; i < rows.length ; i++) {
            this.rows[i] = new MonetDBRow(this, rows[i]);
        }
    }

    /**
     * Gets the number of columns in this set.
     *
     * @return The number of columns in this set
     */
    public int getNumberOfColumns() { return mappings.length; }
}
