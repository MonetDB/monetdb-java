package nl.cwi.monetdb.embedded.tables;

/**
 * Created by ferreira on 11/7/16.
 */
public interface IMonetDBTableRemover extends IMonetDBTableBaseIterator {

    void removeNextRow(RowRemover nextRow);
}
