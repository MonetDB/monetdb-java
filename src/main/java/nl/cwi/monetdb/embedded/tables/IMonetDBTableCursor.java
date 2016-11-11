package nl.cwi.monetdb.embedded.tables;

/**
 * A row iterator for a MonetDB table.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public interface IMonetDBTableCursor extends IMonetDBTableBaseIterator {

    /**
     * The business logic for the iterator.
     *
     * @param nextRow The next row in the iteration.
     */
    void processNextRow(RowIterator nextRow);
}
