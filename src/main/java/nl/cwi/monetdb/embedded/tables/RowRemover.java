package nl.cwi.monetdb.embedded.tables;

/**
 * Created by ferreira on 11/7/16.
 */
public class RowRemover extends RowIterator {

    private boolean toRemove;

    public RowRemover(MonetDBTable table, int firstIndex, int lastIndex) {
        super(table, firstIndex, lastIndex);
        this.toRemove = false;
    }

    public boolean isToRemove() {
        return toRemove;
    }

    public void setToRemove(boolean toRemove) {
        this.toRemove = toRemove;
    }

    @Override
    protected void setNextIteration(Object[] columns, int rowNumber) {
        super.setNextIteration(columns, rowNumber);
        this.toRemove = false;
    }
}
