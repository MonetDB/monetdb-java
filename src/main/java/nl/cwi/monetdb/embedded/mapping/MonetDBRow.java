package nl.cwi.monetdb.embedded.mapping;

import java.util.Arrays;
import java.util.ListIterator;

/**
 * A single MonetDB row in a result set.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class MonetDBRow implements Iterable {

    /**
     * The original row result set from this row.
     */
    private final AbstractRowSet originalSet;

    /**
     * The columns values as Java objects.
     */
    private Object[] columns;

    protected MonetDBRow(AbstractRowSet originalSet, Object[] columns) {
        this.originalSet = originalSet;
        this.columns = columns;
    }

    /**
     * Gets the original row result set from this row.
     *
     * @return The original row result set from this row
     */
    public AbstractRowSet getOriginalSet() { return originalSet; }

    /**
     * Gets the columns values as Java objects.
     *
     * @return The columns values as Java objects
     */
    public Object[] getAllColumns() { return columns; }

    /**
     * Sets all columns values as Java objects.
     *
     * @param values An object array of the elements to update
     */
    public void setAllColumns(Object[] values) {
        if(values.length != this.columns.length)
            throw new ArrayStoreException("The values array and the columns length differ!");
        this.columns = values;
    }

    /**
     * Gets the number of columns.
     *
     * @return The number of columns
     */
    public int getNumberOfColumns() { return columns.length; }

    /**
     * Gets a column value as a Java class.
     *
     * @param <T> A Java class mapped to a MonetDB data type
     * @param index The index of the column
     * @param javaClass The Java class
     * @return The column value as a Java class
     */
    public <T> T getColumn(int index, Class<T> javaClass) { return javaClass.cast(columns[index]); }

    /**
     * Gets a column value as a Java class using the default mapping.
     *
     * @param <T> A Java class mapped to a MonetDB data type
     * @param index The index of the column
     * @return The column value as a Java class
     */
    public <T> T getColumn(int index) {
        Class<T> javaClass = this.originalSet.mappings[index].getJavaClass();
        return javaClass.cast(columns[index]);
    }

    /**
     * Sets a column value as a Java class.
     *
     * @param <T> A Java class mapped to a MonetDB data type
     * @param index The index of the column
     * @param value The value to set
     */
    public <T> void setColumn(int index, T value) {
        this.columns[index] = this.originalSet.mappings[index].getJavaClass().cast(value);
    }

    /**
     * Sets a column value as a Java class.
     *
     * @param <T> A Java class mapped to a MonetDB data type
     * @param index The index of the column
     * @param javaClass The Java class
     * @param value The value to set
     */
    public <T> void setColumn(int index, Class<T> javaClass, T value) {
        this.columns[index] = javaClass.cast(value);
    }

    @Override
    public ListIterator<Object> iterator() { return Arrays.asList(this.columns).listIterator(); }
}
