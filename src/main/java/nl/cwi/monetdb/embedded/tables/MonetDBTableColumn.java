package nl.cwi.monetdb.embedded.tables;

import nl.cwi.monetdb.embedded.mapping.AbstractColumn;

/**
 * Java representation of a MonetDB table column.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class MonetDBTableColumn extends AbstractColumn {

    private final String columnName;

    private final int columnDigits;

    private final int columnScale;

    private final String defaultValue;

    private final boolean isNullable;

    protected MonetDBTableColumn(String columnType, String columnName, int columnDigits, int columnScale,
                                 String defaultValue, boolean isNullable) {
        super(columnType);
        this.columnName = columnName;
        this.columnDigits = columnDigits;
        this.columnScale = columnScale;
        this.defaultValue = defaultValue;
        this.isNullable = isNullable;
    }

    @Override
    public String getColumnName() { return this.columnName; }

    @Override
    public int getColumnDigits() { return this.columnDigits; }

    @Override
    public int getColumnScale() { return this.columnScale; }

    /**
     * Get the default value if there is one, or null if none.
     *
     * @return The default value if there is one, or null if none
     */
    public String getDefaultValue() { return this.defaultValue; }

    /**
     * Get the indication if the column is nullable.
     *
     * @return The indication if the column is nullable
     */
    public boolean isNullable() { return this.isNullable; }
}
