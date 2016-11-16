package nl.cwi.monetdb.embedded.tables;

/**
 * The base interface for iterating a MonetDB Table. The use can specify which rows to iterate in this interface.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public interface IMonetDBTableBaseIterator {

    /**
     * Specify the first row in the table to iterate starting from 1. If a lower number is provided, then the iteration
     * will start on the first row.
     *
     * @return The first row in the table to iterate
     */
    int getFirstRowToIterate();

    /**
     * Specify the last row in the table to iterate. If a number larger than the number of rows is provided, then the
     * iteration will end on the last row of the table.
     *
     * @return The last row in the table to iterate
     */
    int getLastRowToIterate();
}
