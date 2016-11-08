package nl.cwi.monetdb.embedded.tables;

import nl.cwi.monetdb.embedded.mapping.AbstractColumn;

/**
 * Java representation of a MonetDB table column.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class MonetDBTableColumn<T> extends AbstractColumn<T> {

    /**
     * A String representation of the default value if exists, otherwise is null.
     */
    private final String defaultValue;

    /**
     * A boolean indication if the column is nullable.
     */
    private final boolean isNullable;

    public MonetDBTableColumn(int resultSetIndex, String columnName, String columnType, int columnDigits,
                              int columnScale, String defaultValue, boolean isNullable) {
        super(resultSetIndex, columnName, columnType, columnDigits, columnScale);
        this.defaultValue = defaultValue;
        this.isNullable = isNullable;
    }

    /**
     * Get the default value if there is one, or null if none.
     *
     * @return The default value if there is one, or null if none
     */
    public String getDefaultValue() { return defaultValue; }

    /**
     * Get the indication if the column is nullable.
     *
     * @return The indication if the column is nullable
     */
    public boolean isNullable() { return isNullable; }
}
