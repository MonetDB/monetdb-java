package nl.cwi.monetdb.embedded.tables;

import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedException;

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
    private final boolean[][] updatedIndexes;

    protected RowUpdater(MonetDBTable table, Object[][] rows, int firstIndex, int lastIndex) {
        super(table, rows, firstIndex, lastIndex);
        this.updatedIndexes = new boolean[lastIndex - firstIndex][table.getNumberOfColumns()];
    }

    /**
     * Updates a column value.
     *
     * @param <T> A Java class mapped to a MonetDB data type
     * @param index The index of the column
     * @param value The value to set
     */
    public <T> void updateColumn(int index, T value) {
        this.getCurrentRow().setColumn(index, value);
        this.updatedIndexes[this.getCurrentIterationNumber()][index] = true;
    }

    /**
     * Updates a column value.
     *
     * @param <T> A Java class mapped to a MonetDB data type
     * @param index The index of the column
     * @param javaClass The Java class
     * @param value The value to set
     */
    public <T> void updateColumn(int index, Class<T> javaClass, T value) {
        this.getCurrentRow().setColumn(index, javaClass, value);
        this.updatedIndexes[this.getCurrentIterationNumber()][index] = true;
    }

    /**
     * Updates all column values.
     *
     * @param values The values to set
     */
    public void updateAllColumns(Object[] values) {
        this.getCurrentRow().setAllColumns(values);
        Arrays.fill(this.updatedIndexes[this.getCurrentIterationNumber()], true);
    }

    /**
     * Gets a boolean array of the column indexes to be updated in the current iteration.
     *
     * @return A boolean array of the column indexes to be updated in the current iteration
     */
    public boolean[] getCurrentUpdatedIndexes() {
        return Arrays.copyOf(this.updatedIndexes[this.getCurrentIterationNumber()], this.updatedIndexes.length);
    }

    /**
     * Check if the current row is set to be updated.
     *
     * @return A boolean indicating if the current row is to be updated
     */
    public boolean toUpdate() {
        for (boolean bol : this.updatedIndexes[this.getCurrentIterationNumber()]) {
            if(bol) {
                return true;
            }
        }
        return false;
    }

    /**
     * Updates the row set.
     *
     * @return The number of rows updated
     */
    protected native int submitUpdates() throws MonetDBEmbeddedException;
}
