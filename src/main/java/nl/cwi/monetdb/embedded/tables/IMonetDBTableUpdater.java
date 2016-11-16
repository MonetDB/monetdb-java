package nl.cwi.monetdb.embedded.tables;

/**
 * A row update iterator for a MonetDB table.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public interface IMonetDBTableUpdater extends IMonetDBTableBaseIterator {

    /**
     * The business logic for the iterator. Use the
     * {@link nl.cwi.monetdb.embedded.tables.RowUpdater#updateColumnByIndex(int, Class, Object)}
     * method in <code>processNextRow</code> to update the current row.
     *
     * @param nextRow The next row in the iteration.
     */
    void processNextRow(RowUpdater nextRow);
}
