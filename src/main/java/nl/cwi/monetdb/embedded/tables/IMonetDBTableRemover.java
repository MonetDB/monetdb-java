package nl.cwi.monetdb.embedded.tables;

/**
 * A row removal iterator for a MonetDB table.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public interface IMonetDBTableRemover extends IMonetDBTableBaseIterator {

    /**
     * The business logic for the iterator. Use the
     * {@link nl.cwi.monetdb.embedded.tables.RowRemover#setCurrentRowToRemove(boolean) setToRemove}
     * method in <code>processNextRow</code> to set the current row to remove.
     *
     * @param nextRow The next row in the iteration.
     */
    void processNextRow(RowRemover nextRow);
}
