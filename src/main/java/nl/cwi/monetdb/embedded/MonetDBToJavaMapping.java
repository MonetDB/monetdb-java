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
import java.net.InetAddress;
import java.net.URI;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.UUID;

/**
 * A Java enum representing the mappings between MonetDB data types and Java classes.
 *
 * @author <a href="mailto:pedro.ferreira@monetdbsolutions.com">Pedro Ferreira</a>
 */
public enum MonetDBToJavaMapping {

    Boolean(Boolean.class), Char(String.class), Varchar(String.class), Clob(String.class), Tinyint(Byte.class),
    Smallint(Short.class), Int(Integer.class), Bigint(Long.class), Hugeint(BigInteger.class), Decimal(BigDecimal.class),
    Real(Float.class), Double(Double.class), MonthInterval(Integer.class), SecondInterval(Long.class), Time(Time.class),
    TimeTz(Time.class), Date(Date.class), Timestamp(Timestamp.class), TimestampTz(Timestamp.class), Blob(Byte[].class),
    Geometry(Byte[].class), GeometryA(Byte[].class), URL(URI.class), Inet(InetAddress.class), JSON(Byte[].class),
    UUID(UUID.class);

    /**
     * The mapping between MonetDB data types and enum values.
     */
    private static final HashMap<String, MonetDBToJavaMapping> MonetDBMappings;

    static {
        MonetDBMappings = new HashMap<>();
        MonetDBMappings.put("boolean", Boolean);
        MonetDBMappings.put("char", Char);
        MonetDBMappings.put("varchar", Varchar);
        MonetDBMappings.put("clob", Clob);
        MonetDBMappings.put("tinyint", Tinyint);
        MonetDBMappings.put("smallint", Smallint);
        MonetDBMappings.put("int", Int);
        MonetDBMappings.put("bigint", Bigint);
        MonetDBMappings.put("hugeint", Hugeint);
        MonetDBMappings.put("decimal", Decimal);
        MonetDBMappings.put("real", Real);
        MonetDBMappings.put("double", Double);
        MonetDBMappings.put("month_interval", MonthInterval);
        MonetDBMappings.put("sec_interval", SecondInterval);
        MonetDBMappings.put("time", Time);
        MonetDBMappings.put("timetz", TimeTz);
        MonetDBMappings.put("date", Date);
        MonetDBMappings.put("timestamp", Timestamp);
        MonetDBMappings.put("timestamptz", TimestampTz);
        MonetDBMappings.put("blob", Blob);
        MonetDBMappings.put("geometry", Geometry);
        MonetDBMappings.put("geometrya", GeometryA);
        MonetDBMappings.put("url", URL);
        MonetDBMappings.put("inet", Inet);
        MonetDBMappings.put("json", JSON);
        MonetDBMappings.put("uuid", UUID);
    }

    /**
     * Get the corresponding MonetDBToJavaMapping from MonetDB internal data type.
     *
     * @return A MonetDBToJavaMapping enum value
     */
    public static MonetDBToJavaMapping GetJavaMappingFromMonetDBString(String sqlname) {
        return MonetDBMappings.get(sqlname);
    }

    /**
     * The corresponding Java class for the enum value.
     */
    private final Class<?> javaClass;

    MonetDBToJavaMapping(Class<?> javaClass) { this.javaClass = javaClass;}

    /**
     * Gets the corresponding Java class for the enum value.
     *
     * @return The corresponding Java class for the enum value
     */
    @SuppressWarnings("unchecked")
    public <T> Class<T> getJavaClass() {
        return (Class<T>) this.javaClass;
    }

}
