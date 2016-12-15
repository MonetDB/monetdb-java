package nl.cwi.monetdb.mcl.connection.embedded;

import nl.cwi.monetdb.mcl.connection.IMonetDBLanguage;

/**
 * Created by ferreira on 12/9/16.
 */
public enum EmbeddedLanguage implements IMonetDBLanguage {

    /** the SQL language */
    LANG_SQL(null, "sql"),
    /** an unknown language */
    LANG_UNKNOWN(null, "unknown");

    EmbeddedLanguage(String[] queryTemplates, String representation) {
        this.queryTemplates = queryTemplates;
        this.representation = representation;
    }

    private final String[] queryTemplates;

    private final String representation;

    @Override
    public String getQueryTemplateIndex(int index) {
        return queryTemplates[index];
    }

    @Override
    public String getCommandTemplateIndex(int index) {
        return null;
    }

    @Override
    public String[] getQueryTemplates() {
        return queryTemplates;
    }

    @Override
    public String[] getCommandTemplates() {
        return null;
    }

    @Override
    public String getRepresentation() {
        return representation;
    }

    public static EmbeddedLanguage GetLanguageFromString(String language) {
        switch (language) {
            case "sql":
                return LANG_SQL;
            default:
                return LANG_UNKNOWN;
        }
    }
}
