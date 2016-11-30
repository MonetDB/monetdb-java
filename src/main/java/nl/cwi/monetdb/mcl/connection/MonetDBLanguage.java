package nl.cwi.monetdb.mcl.connection;

/**
 * Created by ferreira on 11/30/16.
 */
public enum MonetDBLanguage {

    /** the SQL language */
    LANG_SQL(new byte[][]{"s".getBytes(), "\n;".getBytes(), "\n;\n".getBytes()}, new byte[][]{"X".getBytes(), null, "\nX".getBytes()}),
    /** the MAL language (officially *NOT* supported) */
    LANG_MAL(new byte[][]{null, ";\n".getBytes(), ";\n".getBytes()}, new byte[][]{null, null, null}),
    /** an unknown language */
    LANG_UNKNOWN(null, null);

    MonetDBLanguage(byte[][] queryTemplate, byte[][] commandTemplate) {
        this.queryTemplate = queryTemplate;
        this.commandTemplate = commandTemplate;
    }

    private final byte[][] queryTemplate;

    private final byte[][] commandTemplate;

    public byte[] getQueryTemplateIndex(int index) {
        return queryTemplate[index];
    }

    public byte[] getCommandTemplateIndex(int index) {
        return commandTemplate[index];
    }
}
