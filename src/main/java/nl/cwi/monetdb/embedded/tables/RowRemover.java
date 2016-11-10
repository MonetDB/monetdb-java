package nl.cwi.monetdb.embedded.tables;

import nl.cwi.monetdb.embedded.env.MonetDBEmbeddedException;

/**
 * The removal iterator for a MonetDB table.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class RowRemover extends RowIterator {

    /**
     * If the next row is going to be removed.
     */
    private boolean[] removeIndexes;

    public RowRemover(MonetDBTable table, Object[][] rows, int firstIndex, int lastIndex) {
        super(table, rows, firstIndex, lastIndex);
        this.removeIndexes = new boolean[lastIndex - firstIndex];
    }

    /**
     * Checks if the next row is going to be removed.
     *
     * @return If the next row is going to be removed
     */
    public boolean isCurrentRowSetToRemove() { return this.removeIndexes[this.getCurrentIterationNumber()]; }

    /**
     * Sets the current row to remove or not.
     *
     * @param toRemove A boolean indicating if the next row will be removed
     */
    public void setCurrentRowToRemove(boolean toRemove) { this.removeIndexes[this.getCurrentIterationNumber()] = toRemove; }

    /**
     * Removes the rows.
     *
     * @return The number of rows deleted
     */
    protected native int submitDeletes() throws MonetDBEmbeddedException;
}
