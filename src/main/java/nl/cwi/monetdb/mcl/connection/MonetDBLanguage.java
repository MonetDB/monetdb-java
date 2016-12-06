package nl.cwi.monetdb.mcl.connection;

/**
 * Created by ferreira on 11/30/16.
 */
public enum MonetDBLanguage {

    /** the SQL language */
    LANG_SQL(new byte[][]{"s".getBytes(), "\n;".getBytes(), "\n;\n".getBytes()}, new byte[][]{"X".getBytes(), null, "\nX".getBytes()}, "sql"),
    /** the MAL language (officially *NOT* supported) */
    LANG_MAL(new byte[][]{null, ";\n".getBytes(), ";\n".getBytes()}, new byte[][]{null, null, null}, "mal"),
    /** an unknown language */
    LANG_UNKNOWN(null, null, "unknown");

    MonetDBLanguage(byte[][] queryTemplates, byte[][] commandTemplates, String representation) {
        this.queryTemplates = queryTemplates;
        this.commandTemplates = commandTemplates;
        this.representation = representation;
    }

    private final byte[][] queryTemplates;

    private final byte[][] commandTemplates;

    private final String representation;

    public byte[] getQueryTemplateIndex(int index) {
        return queryTemplates[index];
    }

    public byte[] getCommandTemplateIndex(int index) {
        return commandTemplates[index];
    }

    public byte[][] getQueryTemplates() {
        return queryTemplates;
    }

    public byte[][] getCommandTemplates() {
        return commandTemplates;
    }

    public String getRepresentation() {
        return representation;
    }

    public static final byte[] EmptyString = "".getBytes();

    public static MonetDBLanguage GetLanguageFromString(String language) {
        switch (language) {
            case "sql":
                return LANG_SQL;
            case "mal":
                return LANG_MAL;
            default:
                return LANG_UNKNOWN;
        }
    }
}
