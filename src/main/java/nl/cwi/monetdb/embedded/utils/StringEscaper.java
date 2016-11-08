package nl.cwi.monetdb.embedded.utils;

/**
 * An util class to escape Java Strings to avoid SQL Injection and other problems with SQL queries.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public class StringEscaper {

    /**
     * Escapes a Java String for usage in SQL queries.
     *
     * @param input The String to escape
     * @return The input String escaped
     */
    public static String SQLStringEscape(String input) {
        return "'" + input.replaceAll("\\\\", "\\\\\\\\").replaceAll("'", "\\\\'") + "'";
    }
}
