package nl.cwi.monetdb.embedded.tables;

import nl.cwi.monetdb.embedded.mapping.MonetDBToJavaMapping;

/**
 * Created by ferreira on 11/7/16.
 */
public class RowIterator {

    /**
     * The original table of this iterator.
     */
    protected final MonetDBTable table;

    /**
     * The mappings of the columns.
     */
    protected final MonetDBToJavaMapping[] mappings;

    /**
     * The columns values as Java objects.
     */
    protected Object[] columns;

    /**
     * The current row number.
     */
    protected int rowNumber;

    private final int firstIndex;

    private final int lastIndex;

    public RowIterator(MonetDBTable table, int firstIndex, int lastIndex) {
        this.table = table;
        this.mappings = table.getMappings();
        this.firstIndex = Math.max(firstIndex, 0);
        this.lastIndex = Math.min(lastIndex, table.getNumberOfRows());
    }

    /**
     * Gets the original table of this iterator.
     *
     * @return The original table of this iterator
     */
    public MonetDBTable getTable() { return table; }

    /**
     * Gets the columns values as Java objects.
     *
     * @return The columns values as Java objects
     */
    public Object[] getColumns() {
        return columns;
    }

    /**
     * Gets the current row number in the iteration.
     *
     * @return The current row number in the iteration
     */
    public int getRowNumber() { return rowNumber; }

    /**
     * Gets a column value as a Java class.
     *
     * @param <T> A Java class mapped to a MonetDB data type
     * @param index The index of the column
     * @param javaClass The Java class
     * @return The column value as a Java class
     */
    public <T> T getColumn(int index, Class<T> javaClass) {
        return javaClass.cast(columns[index]);
    }

    /**
     * Gets a column value as a Java class using the default mapping.
     *
     * @param <T> A Java class mapped to a MonetDB data type
     * @param index The index of the column
     * @return The column value as a Java class
     */
    public <T> T getColumn(int index) {
        Class<T> javaClass = this.mappings[index].getJavaClass();
        return javaClass.cast(columns[index]);
    }

    protected void setNextIteration(Object[] columns, int rowNumber) {
        this.columns = columns;
        this.rowNumber = rowNumber;
    }
}
