package nl.cwi.monetdb.embedded.tables;

import java.util.Arrays;

/**
 * Created by ferreira on 11/7/16.
 */
public class RowUpdater extends RowIterator {

    private final boolean[] updatedIndexes;

    public RowUpdater(MonetDBTable table, int firstIndex, int lastIndex) {
        super(table, firstIndex, lastIndex);
        this.updatedIndexes = new boolean[table.getNumberOfColumns()];
    }

    public <T> void setColumn(int index, T value) {
        this.columns[index] = value;
        this.updatedIndexes[index] = true;
    }

    public <T> void setColumn(int index, Class<T> javaClass, T value) {
        this.columns[index] = value;
        this.updatedIndexes[index] = true;
    }

    public void setAllColumns(Object[] values) {
        this.columns = values;
        Arrays.fill(this.updatedIndexes, true);
    }

    public boolean toUpdate() {
        for (boolean bol : updatedIndexes) {
            if(bol) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected void setNextIteration(Object[] columns, int rowNumber) {
        super.setNextIteration(columns, rowNumber);
        Arrays.fill(this.updatedIndexes, false);
    }
}
