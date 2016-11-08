package nl.cwi.monetdb.embedded.tables;

import nl.cwi.monetdb.embedded.mapping.MonetDBToJavaMapping;

/**
 * The iterator class for a MonetDB table. It's possible to inspect the current currentColumns in the row as well
 * their mappings.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class RowIterator {

    /**
     * The original table of this iterator.
     */
    protected final MonetDBTable table;

    /**
     * The mappings of the currentColumns.
     */
    protected final MonetDBToJavaMapping[] mappings;

    /**
     * The currentColumns values as Java objects.
     */
    protected Object[] currentColumns;

    /**
     * The current row number.
     */
    protected int currentRowNumber;

    /**
     * The first row in the table to iterate.
     */
    protected final int firstIndex;

    /**
     * The last row in the table to iterate.
     */
    protected final int lastIndex;

    public RowIterator(MonetDBTable table, int firstIndex, int lastIndex) {
        this.table = table;
        this.mappings = table.getMappings();
        this.firstIndex = Math.max(firstIndex, 0);
        this.lastIndex = Math.min(Math.min(lastIndex, table.getNumberOfRows()), 0);
        this.currentRowNumber = this.firstIndex - 1; //starting on the row before the first index
    }

    /**
     * Gets the original table of this iterator.
     *
     * @return The original table of this iterator
     */
    public MonetDBTable getTable() { return table; }

    /**
     * Gets the current row currentColumns values as Java objects.
     *
     * @return The current row currentColumns values as Java objects
     */
    public Object[] getCurrentColumns() { return currentColumns; }

    /**
     * Gets the current row number in the iteration.
     *
     * @return The current row number in the iteration
     */
    public int getCurrentRowNumber() { return currentRowNumber; }

    /**
     * Gets the first index used on this iteration.
     *
     * @return The first index used on this iteration
     */
    public int getFirstIndex() { return firstIndex; }

    /**
     * Gets the last index used on this iteration.
     *
     * @return The last index used on this iteration
     */
    public int getLastIndex() { return lastIndex; }

    /**
     * Checks if there are more rows to iterate after the current one.
     *
     * @return There are more rows to iterate
     */
    public boolean hasMore() { return currentRowNumber < lastIndex; }

    /**
     * Gets a column value as a Java class.
     *
     * @param <T> A Java class mapped to a MonetDB data type
     * @param index The index of the column
     * @param javaClass The Java class
     * @return The column value as a Java class
     */
    public <T> T getColumn(int index, Class<T> javaClass) { return javaClass.cast(this.currentColumns[index]); }

    /**
     * Gets a column value as a Java class using the default mapping.
     *
     * @param <T> A Java class mapped to a MonetDB data type
     * @param index The index of the column
     * @return The column value as a Java class
     */
    public <T> T getColumn(int index) {
        Class<T> javaClass = this.mappings[index].getJavaClass();
        return javaClass.cast(this.currentColumns[index]);
    }

    /**
     * Method used by JNI to set the next columns and increment the current row number.
     *
     * @param columns The next retrieved columns
     */
    protected void setNextIteration(Object[] columns) {
        this.currentColumns = columns;
        this.currentRowNumber++;
    }

    /**
     * Gets the next row in the iteration if there are more.
     *
     * @return A boolean indicating if a row was fetched
     */
    protected native boolean getNextTableRow();
}
