/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.embedded.mapping;

/**
 * A single Java representation of a MonetDB column.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public abstract class AbstractColumn {

    /**
     * The Mapping between MonetDB type and the Java Class.
     */
    protected final MonetDBToJavaMapping mapping;

    protected AbstractColumn(String columnType) {
        this.mapping = MonetDBToJavaMapping.GetJavaMappingFromMonetDBString(columnType);
    }

    /**
     * Gets the type of the column.
     *
     * @return The Column type
     */
    public String getColumnInternalTypeName() { return mapping.toString(); }

    /**
     * Gets the Java mapping of the column.
     *
     * @return A enum constant of the Java mapping
     */
    public MonetDBToJavaMapping getMapping() { return mapping; }

    /**
     * Gets the name of the column.
     *
     * @return The column name
     */
    public abstract String getColumnName();

    /**
     * Gets the number digits of the column.
     *
     * @return The number of digits
     */
    public abstract int getColumnDigits();

    /**
     * Gets the scale of the column.
     *
     * @return The scale
     */
    public abstract int getColumnScale();
}
