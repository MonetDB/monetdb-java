package nl.cwi.monetdb.embedded.column;

import nl.cwi.monetdb.embedded.EmbeddedQueryResult;

/**
 * Mapping for MonetDB VARCHAR data type
 */
public class VarcharColumn extends Column<String> {

    private final String[] values;

    public VarcharColumn(EmbeddedQueryResult result, int index, String[] values, boolean[] nullIndex) {
        super(result, index, nullIndex);
        this.values = values;
    }

    @Override
    public String[] getAllValues() {
        return this.values;
    }

    @Override
    protected String getValueImplementation(int index) {
        return this.values[index];
    }
}
