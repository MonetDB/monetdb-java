package nl.cwi.monetdb.embedded.tables;

import nl.cwi.monetdb.embedded.mapping.AbstractColumn;

/**
 * Java representation of a MonetDB table column.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class MonetDBTableColumn<T> extends AbstractColumn<T> {

    public MonetDBTableColumn(int resultSetIndex, String columnName, String columnType, int columnDigits,
                              int columnScale) {
        super(resultSetIndex, columnName, columnType, columnDigits, columnScale);
    }

}
