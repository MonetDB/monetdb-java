package nl.cwi.monetdb.embedded.tables;

/**
 * Created by ferreira on 11/7/16.
 */
public interface IMonetDBTableUpdater extends IMonetDBTableBaseIterator {

    void updateNextRow(RowUpdater nextRow);
}
