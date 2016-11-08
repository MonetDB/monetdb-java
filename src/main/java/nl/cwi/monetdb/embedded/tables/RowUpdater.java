package nl.cwi.monetdb.embedded.tables;

import java.util.Arrays;

/**
 * The update iterator for a MonetDB table.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class RowUpdater extends RowIterator {

    /**
     * A boolean array to check the columns to be updated.
     */
    private final boolean[] updatedIndexes;

    public RowUpdater(MonetDBTable table, int firstIndex, int lastIndex) {
        super(table, firstIndex, lastIndex);
        this.updatedIndexes = new boolean[table.getNumberOfColumns()];
    }

    /**
     * Sets a column value as a Java class.
     *
     * @param <T> A Java class mapped to a MonetDB data type
     * @param index The index of the column
     * @param value The value to set
     */
    public <T> void setColumn(int index, T value) {
        this.currentColumns[index] = this.mappings[index].getJavaClass().cast(value);
        this.updatedIndexes[index] = true;
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
        this.currentColumns[index] = javaClass.cast(value);
        this.updatedIndexes[index] = true;
    }

    /**
     * Sets all column values as Java classes.
     *
     * @param values The values to set
     */
    public void setAllColumns(Object[] values) {
        if(values.length != this.currentColumns.length)
            throw new ArrayStoreException("The values array and the columns length differ!");
        this.currentColumns = values;
        Arrays.fill(this.updatedIndexes, true);
    }

    /**
     * Gets a boolean array of the columns indexes to be updated.
     *
     * @return A boolean array of the columns indexes to be updated
     */
    public boolean[] getUpdatedIndexes() { return Arrays.copyOf(this.updatedIndexes, this.updatedIndexes.length); }

    /**
     * Check if the current row is to be updated.
     *
     * @return A boolean indicating if the current row is to be updated
     */
    public boolean toUpdate() {
        for (boolean bol : this.updatedIndexes) {
            if(bol) {
                return true;
            }
        }
        return false;
    }

    /**
     * To be called by the JNI interface in every iteration.
     *
     * @param columns The next row's columns
     */
    @Override
    protected void setNextIteration(Object[] columns) {
        super.setNextIteration(columns);
        Arrays.fill(this.updatedIndexes, false);
    }

    /**
     * Update the current row if there are changes.
     *
     * @return If the row was updated internally
     */
    protected boolean tryUpdate() { return this.toUpdate() && this.updateNextTableRow(); }

    /**
     * Updates the next row.
     *
     * @return If the row was updated internally
     */
    private native boolean updateNextTableRow();
}
