/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * The embedded version of the {@link PreparedStatement} interface from JDBC (not inheriting for simpler implementation).
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>, Fabian Groffen, Martin van Dinther
 */
public class EmbeddedPreparedStatement {

    /**
     * A formatter for Time columns without timezone.
     */
    private static final SimpleDateFormat TimeFormatter;

    /**
     * A formatter for Time columns with timezone.
     */
    private static final SimpleDateFormat TimeTzFormatter;

    /**
     * A formatter for Timestamp columns without timezone.
     */
    private static final SimpleDateFormat TimestampFormatter;

    /**
     * A formatter for Timestamp columns with timezone.
     */
    private static final SimpleDateFormat TimestampTzFormatter;

    /**
     * A list of Java classes that don't need special parsing of values (jsut call toString() method).
     */
    private static final List<Class<?>> DirectMappingClasses;

    static {
        TimeFormatter = new SimpleDateFormat("HH:mm:ss.SSS");
        TimeTzFormatter = new SimpleDateFormat("HH:mm:ss.SSSZ");
        TimestampFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        TimestampTzFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
        DirectMappingClasses = new ArrayList<>();
        DirectMappingClasses.add(Boolean.class);
        DirectMappingClasses.add(Byte.class);
        DirectMappingClasses.add(Short.class);
        DirectMappingClasses.add(Integer.class);
        DirectMappingClasses.add(Long.class);
        DirectMappingClasses.add(BigInteger.class);
        DirectMappingClasses.add(Float.class);
        DirectMappingClasses.add(Double.class);
        DirectMappingClasses.add(Byte[].class);
        DirectMappingClasses.add(URI.class);
        DirectMappingClasses.add(InetAddress.class);
        DirectMappingClasses.add(UUID.class);
    }

    /**
     * The corresponding connection for this Prepared statement.
     */
    private final MonetDBEmbeddedConnection connection;

    /**
     * The id of the generated prepare statement.
     */
    private final int prepareId;

    /**
     * The number of parsed parameters (?) in the statement.
     */
    private final int numberOfParameters;

    /**
     * The list of MonetDB-to-Java mappings of the parameters.
     */
    private final MonetDBToJavaMapping[] parametersMappings;

    /**
     * The number of digits allowed for each parameter.
     */
    private final int[] parametersDigits;

    /**
     * The scale for each parameter.
     */
    private final int[] parametersScales;

    /**
     * The schema name corresponding for each parameter (or null if not corresponding).
     */
    private final String[] parametersSchemas;

    /**
     * The table name corresponding for each parameter (or null if not corresponding).
     */
    private final String[] parametersTables;

    /**
     * The column name corresponding for each parameter (or null if not corresponding).
     */
    private final String[] parametersNames;

    /**
     * The parsed value for each parameter (or null if not parsed).
     */
    private final String[] parsedValues;

    protected EmbeddedPreparedStatement(MonetDBEmbeddedConnection connection, int prepareId,
                                        MonetDBToJavaMapping[] parametersMappings, int[] parametersDigits,
                                        int[] parametersScales, String[] parametersSchemas, String[] parametersTables,
                                        String[] parametersNames, String[] parsedValues) throws MonetDBEmbeddedException {
        this.connection = connection;
        this.prepareId = prepareId;
        this.numberOfParameters = parametersMappings.length;
        this.parametersMappings = parametersMappings;
        this.parametersDigits = parametersDigits;
        this.parametersScales = parametersScales;
        this.parametersSchemas = parametersSchemas;
        this.parametersTables = parametersTables;
        this.parametersNames = parametersNames;
        this.parsedValues = parsedValues;
    }

    /**
     * Get the corresponding connection for this statement.
     *
     * @return A MonetDBEmbeddedConnection instance
     */
    public MonetDBEmbeddedConnection getConnection() {
        return connection;
    }

    /**
     * Get the number of parsed parameters (?) in the statement.
     *
     * @return The number of parsed parameters (?) in the statement
     */
    public int getNumberOfParameters() { return numberOfParameters; }

    /**
     * Gets the list of MonetDB-to-Java mappings of the parameters.
     *
     * @return The list of MonetDB-to-Java mappings of the parameters
     */
    public MonetDBToJavaMapping[] getParametersMappings() {
        return parametersMappings;
    }

    /**
     * Gets the number of digits allowed for each parameter.
     *
     * @return The number of digits allowed for each parameter
     */
    public int[] getParametersDigits() {
        return parametersDigits;
    }

    /**
     * Gets the scale for each parameter.
     *
     * @return The scale for each parameter
     */
    public int[] getParametersScales() {
        return parametersScales;
    }

    /**
     * Gets the schema name corresponding for each parameter (or null if not corresponding).
     *
     * @return The schema name corresponding for each parameter (or null if not corresponding)
     */
    public String[] getParametersSchemas() {
        return parametersSchemas;
    }

    /**
     * Gets the table name corresponding for each parameter (or null if not corresponding).
     *
     * @return The table name corresponding for each parameter (or null if not corresponding)
     */
    public String[] getParametersTables() {
        return parametersTables;
    }

    /**
     * Gets the column name corresponding for each parameter (or null if not corresponding).
     *
     * @return The column name corresponding for each parameter (or null if not corresponding)
     */
    public String[] getParametersNames() {
        return parametersNames;
    }

    /**
     * Gets the parsed value for each parameter (or null if not parsed).
     *
     * @return The parsed value for each parameter (or null if not parsed)
     */
    public String[] getParsedValues() {
        return parsedValues;
    }

    /**
     * Clears the current parsed values.
     */
    public void clearParameters() {
        for (int i = 0; i < parsedValues.length; i++) {
            parsedValues[i] = null;
        }
    }

    /**
     * Converts a Java String into the MonetDB representation according to its type.
     *
     * @param parameter The index of the parameter
     * @param value A String value to parse
     * @return The parsed String as a String
     * @throws MonetDBEmbeddedException If the type is a char or varchar an the length is longer than allowed
     */
    private String setString(int parameter, String value) throws MonetDBEmbeddedException {
        String type = this.parametersMappings[parameter].toString();
        if((type.equals("Char") || type.equals("Varchar")) && value.length() > this.parametersDigits[parameter]) {
            throw new MonetDBEmbeddedException("The length is higher than allowed: " + value.length() + " > "
                    + this.parametersDigits[parameter] + "!");
        } else {
            return value;
        }
    }

    /**
     * Converts a Java BigDecimal into a MonetDB decimal representation (adapted from the JDBC driver implementation).
     *
     * @param parameter The index of the parameter
     * @param value A BigDecimal value to parse
     * @return The parsed BigDecimal as a String
     * @throws MonetDBEmbeddedException If the value exceeds the allowed digits or scale
     */
    private String setBigDecimal(int parameter, BigDecimal value) throws MonetDBEmbeddedException {
        // round to the scale of the DB:
        value = value.setScale(this.parametersScales[parameter], RoundingMode.HALF_UP);
        if (value.precision() > this.parametersDigits[parameter]) {
            throw new MonetDBEmbeddedException("DECIMAL value exceeds allowed digits/scale: " + value.toPlainString()
                    + " (" + this.parametersDigits[parameter] + "/" + this.parametersScales[parameter] + ")");
        }

        // MonetDB doesn't like leading 0's, since it counts them as part of
        // the precision, so let's strip them off. (But be careful not to do
        // this to the exact number "0".)  Also strip off trailing
        // numbers that are inherent to the double representation.
        String xStr = value.toPlainString();
        int dot = xStr.indexOf('.');
        if (dot >= 0)
            xStr = xStr.substring(0, Math.min(xStr.length(), dot + 1 + this.parametersScales[parameter]));
        while (xStr.startsWith("0") && xStr.length() > 1)
            xStr = xStr.substring(1);
        return xStr;
    }

    /**
     * Converts a Java SQL Time into a MonetDB time representation (adapted from the JDBC driver implementation).
     *
     * @param parameter The index of the parameter
     * @param value A Java SQL Time value to parse
     * @return The parsed Java SQL Time as a String
     */
    private String setTime(int parameter, Time value) {
        boolean hasTimeZone = this.parametersMappings[parameter].toString().endsWith("Tz");
        if(hasTimeZone) {
            String RFC822 = TimeTzFormatter.format(value);
            return RFC822.substring(0, 15) + ":" + RFC822.substring(15);
        } else {
            return TimeFormatter.format(value);
        }
    }

    /**
     * Converts a Java SQL Timestamp into a MonetDB time representation (adapted from the JDBC driver implementation).
     *
     * @param parameter The index of the parameter
     * @param value A Java SQL Timestamp value to parse
     * @return The parsed Java SQL Timestamp as a String
     */
    private String setTimestamp(int parameter, Timestamp value) {
        boolean hasTimeZone = this.parametersMappings[parameter].toString().endsWith("Tz");
        if(hasTimeZone) {
            String RFC822 = TimestampTzFormatter.format(value);
            return RFC822.substring(0, 26) + ":" + RFC822.substring(26);
        } else {
            return TimestampFormatter.format(value);
        }
    }

    /**
     * Converts a Java Class into the corresponding MonetDB representation in a parameter.
     *
     * @param <T> The Java Class to map to MonetDB
     * @param parameter The index of the parameter
     * @param value The instance of the Java class to map
     * @throws MonetDBEmbeddedException If the Java class has no mapping in a MonetDB datatype
     */
    public <T> void setParameterValue(int parameter, T value) throws MonetDBEmbeddedException {
        this.setParameterValue(parameter, value, this.parametersMappings[parameter].getJavaClass());
    }

    /**
     * Converts a Java Class into the corresponding MonetDB representation in a parameter.
     *
     * @param <T> The Java Class to map to MonetDB
     * @param parameter The index of the parameter
     * @param value The instance of the Java class to map
     * @param javaClass The class of the instance
     * @throws MonetDBEmbeddedException If the Java class has no mapping in a MonetDB datatype
     */
    public <T> void setParameterValue(int parameter, T value, Class<T> javaClass) throws MonetDBEmbeddedException {
        if(value == null) {
            this.setParameterNull(parameter);
        } else {
            String valueToSubmit;
            if(DirectMappingClasses.contains(javaClass)) {
                valueToSubmit = value.toString();
            } else if(javaClass.equals(String.class)) {
                valueToSubmit = this.setString(parameter, (String) value);
            } else if(javaClass.equals(BigDecimal.class)) {
                valueToSubmit = this.setBigDecimal(parameter, (BigDecimal) value);
            } else if(javaClass.equals(Time.class)) {
                valueToSubmit = this.setTime(parameter, (Time) value);
            } else if(javaClass.equals(Timestamp.class)) {
                valueToSubmit = this.setTimestamp(parameter, (Timestamp) value);
            } else {
                throw new MonetDBEmbeddedException("The class " + javaClass.getSimpleName() +
                        " is not supported by the mapping!");
            }
            this.parsedValues[parameter] = "'" + valueToSubmit.replaceAll("\\\\", "\\\\\\\\")
                    .replaceAll("'", "\\\\'") + "'";
        }
    }

    /**
     * Set a parameter as a null value.
     *
     * @param parameter The index of the parameter
     */
    public void setParameterNull(int parameter) {
        this.parsedValues[parameter] = "NULL";
    }

    /**
     * Creates the SQL String from the parsed parameters (adapted from the JDBC driver implementation).
     *
     * @throws MonetDBEmbeddedException If a parameter has not been set yet
     */
    private String applyParameters() throws MonetDBEmbeddedException {
        StringBuilder buf = new StringBuilder(8 + 12 * this.numberOfParameters).append("exec ")
                .append(this.prepareId).append('(');
        int col = 0;
        for (int i = 0; i < this.numberOfParameters; i++) {
            if (this.parametersNames[i] != null)
                continue;
            col++;
            if (col > 1)
                buf.append(',');
            if (this.parsedValues[i] == null) {
                throw new MonetDBEmbeddedException("Cannot execute, parameter " + col + " is missing!");
            }
            buf.append(this.parsedValues[i]);
        }
        buf.append(')');
        return buf.toString();
    }

    /**
     * Executes this statement as a SQL query without a result set.
     *
     * @return The update result object
     * @throws MonetDBEmbeddedException If an error in the database occurred or if a parameter has not been set yet
     */
    public UpdateResultSet sendUpdate() throws MonetDBEmbeddedException {
        return this.connection.sendUpdate(this.applyParameters());
    }

    /**
     * Executes this statement as a SQL query with a result set.
     *
     * @return The query result object
     * @throws MonetDBEmbeddedException If an error in the database occurred or if a parameter has not been set yet
     */
    public QueryResultSet sendQuery() throws MonetDBEmbeddedException {
        return this.connection.sendQuery(this.applyParameters());
    }

    /**
     * Executes this statement as a SQL query without a result set asynchronously.
     *
     * @return The update result object
     * @throws MonetDBEmbeddedException If an error in the database occurred or if a parameter has not been set yet
     */
    public UpdateResultSet sendUpdateAsync() throws MonetDBEmbeddedException {
        return this.connection.sendUpdateAsync(this.applyParameters());
    }

    /**
     * Executes this statement as a SQL query with a result set asynchronously.
     *
     * @return The query result object
     * @throws MonetDBEmbeddedException If an error in the database occurred or if a parameter has not been set yet
     */
    public QueryResultSet sendQueryAsync() throws MonetDBEmbeddedException {
        return this.connection.sendQueryAsync(this.applyParameters());
    }
}
