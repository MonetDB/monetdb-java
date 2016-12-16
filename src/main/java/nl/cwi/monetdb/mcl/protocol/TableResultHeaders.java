package nl.cwi.monetdb.mcl.protocol;

/**
 * Created by ferreira on 11/30/16.
 */
public enum TableResultHeaders {

    /* Please don't change the order */
    NAME(1),
    LENGTH(2),
    TABLE(4),
    TYPE(8),
    ALL(15),
    UNKNOWN(0);

    private final int valueForBitMap;

    TableResultHeaders(int valueForBitMap) {
        this.valueForBitMap = valueForBitMap;
    }

    public int getValueForBitMap() {
        return valueForBitMap;
    }
}
