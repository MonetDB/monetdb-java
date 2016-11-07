package nl.cwi.monetdb.embedded.tables;

/**
 * Created by ferreira on 11/7/16.
 */
public interface IMonetDBTableIterator extends IMonetDBTableBaseIterator {

    void nextRow(RowIterator nextRow);
}
