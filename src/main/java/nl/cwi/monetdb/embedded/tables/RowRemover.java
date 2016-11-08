package nl.cwi.monetdb.embedded.tables;

/**
 * The removal iterator for a MonetDB table.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class RowRemover extends RowIterator {

    /**
     * If the next row is going to be removed.
     */
    private boolean toRemove;

    public RowRemover(MonetDBTable table, int firstIndex, int lastIndex) {
        super(table, firstIndex, lastIndex);
        this.toRemove = false;
    }

    /**
     * Checks if the next row is going to be removed.
     *
     * @return If the next row is going to be removed
     */
    public boolean isToRemove() { return toRemove; }

    /**
     * Sets the current row to remove or not.
     *
     * @param toRemove A boolean indicating if the next row will be removed
     */
    public void setToRemove(boolean toRemove) { this.toRemove = toRemove; }

    /**
     * To be called by the JNI interface in every iteration.
     *
     * @param columns The next row's columns
     */
    @Override
    protected void setNextIteration(Object[] columns) {
        super.setNextIteration(columns);
        this.toRemove = false;
    }

    /**
     * Remove the current row if it was set for so.
     *
     * @return If the row was removed internally
     */
    protected boolean tryRemove() { return this.isToRemove() && this.removeNextTableRow(); }

    /**
     * Removes the next row.
     *
     * @return If the row was removed internally
     */
    private native boolean removeNextTableRow();
}
