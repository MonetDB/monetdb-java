/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

package org.monetdb.util;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

public final class SQLExporter extends Exporter {
	private int outputMode;
	private Stack<String> lastSchema;

	public final static short TYPE_OUTPUT  = 1;
	public final static short VALUE_INSERT = 0;
	public final static short VALUE_COPY   = 1;
	public final static short VALUE_TABLE  = 2;

	public SQLExporter(final java.io.PrintWriter out) {
		super(out);
	}

	/**
	 * A helper method to generate SQL CREATE code for a given table.
	 * This method performs all required lookups to find all relations and
	 * column information, as well as additional indices.
	 *
	 * @param dbmd a DatabaseMetaData object to query on (not null)
	 * @param type the type of the object, e.g. VIEW, TABLE (not null)
	 * @param schema the schema the object is in (not null)
	 * @param name the table to describe in SQL CREATE format (not null)
	 * @throws SQLException if a database related error occurs
	 */
	public void dumpSchema(
			final DatabaseMetaData dbmd,
			final String type,
			final String schema,
			final String name)
		throws SQLException
	{
		assert dbmd != null;
		assert type != null;
		assert schema != null;
		assert name != null;

		final String fqname = dq(schema) + "." + dq(name);

		if (useSchema)
			changeSchema(schema);

		// handle views directly
		if (type.endsWith("VIEW")) {	// for types: VIEW and SYSTEM VIEW
			final String viewDDL = fetchSysTablesQueryValue(dbmd.getConnection(), schema, name);
			if (viewDDL != null)
				out.println(viewDDL);
			else
				out.println("-- unknown " + type + " " + fqname + ": no SQL view definition found!");
			return;
		}

		out.println("CREATE " + type + " " + fqname + " (");

		// add all columns with their type, nullability and default definition
		ResultSet cols = dbmd.getColumns(null, schema, name, null);
		int colNmIndex = cols.findColumn("COLUMN_NAME");
		final int colTypeNmIndex = cols.findColumn("TYPE_NAME");
		final int datatypeIndex = cols.findColumn("DATA_TYPE");
		final int sizeIndex = cols.findColumn("COLUMN_SIZE");
		final int digitsIndex = cols.findColumn("DECIMAL_DIGITS");
		final int isNotNullIndex = cols.findColumn("NULLABLE");
		final int defaultValueIndex = cols.findColumn("COLUMN_DEF");
		final ResultSetMetaData rsmd = cols.getMetaData();
		final int colwidth = rsmd.getColumnDisplaySize(colNmIndex);
		int typewidth = rsmd.getColumnDisplaySize(colTypeNmIndex);
		if (typewidth < 13)
			typewidth = 13;	// use minimal 13 characters for the typename (same as used in mclient)

		final StringBuilder sb = new StringBuilder(128);
		int i;
		for (i = 0; cols.next(); i++) {
			if (i > 0)
				out.println(",");

			// print column name (with double quotes)
			String s = dq(cols.getString(colNmIndex));
			out.print("\t" + s + repeat(' ', (colwidth - s.length() + 3)));

			int digits = cols.getInt(digitsIndex);
			s = cols.getString(colTypeNmIndex).toUpperCase();	// ANSI SQL uses uppercase data type names
			// do some data type substitutions to match SQL standard
			if (s.equals("INT")) {
				s = "INTEGER";
			} else if (s.equals("SEC_INTERVAL")) {
				s = "INTERVAL SECOND";
			} else if (s.equals("DAY_INTERVAL")) {
				s = "INTERVAL DAY";
			} else if (s.equals("MONTH_INTERVAL")) {
				s = "INTERVAL MONTH";
			} else if (s.equals("TIMETZ")) {
				s = "TIME";
				// small hack to get desired behaviour: set digits when we have
				// a time with time zone and at the same time masking the internal types
				digits = 1;
			} else if (s.equals("TIMESTAMPTZ")) {
				s = "TIMESTAMP";
				// small hack to get desired behaviour: set digits when we have
				// a timestamp with time zone and at the same time masking the internal types
				digits = 1;
			}
			sb.append(s);	// add the data type for this column

			int ctype = cols.getInt(datatypeIndex);
			int size = cols.getInt(sizeIndex);
			// do some SQL/MonetDB type length/precision and scale specifics
			switch (ctype) {
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
				case Types.CLOB:
				case Types.BLOB:
				case Types.FLOAT:
					if (size > 0)
						sb.append('(').append(size).append(')');
					break;
				case Types.TIME:
				case Types.TIME_WITH_TIMEZONE:
				case Types.TIMESTAMP:
				case Types.TIMESTAMP_WITH_TIMEZONE:
					if (size > 1)
						sb.append('(').append(size -1).append(')');
					if (digits == 1)	// flag is set to include suffix: WITH TIME ZONE
						sb.append(" WITH TIME ZONE");
					break;
				case Types.DECIMAL:
				case Types.NUMERIC:
					sb.append('(').append(size);
					if (digits != 0)
						sb.append(',').append(digits);
					sb.append(')');
					break;
			}

			boolean isNotNull = cols.getInt(isNotNullIndex) == DatabaseMetaData.columnNoNulls;
			String defaultValue = cols.getString(defaultValueIndex);
			boolean hasDefault = (defaultValue != null && !defaultValue.isEmpty());
			if (isNotNull || hasDefault) {
				final int spaces = typewidth - sb.length();
				if (spaces > 0)
					sb.append(repeat(' ', spaces));
				if (isNotNull)
					sb.append(" NOT NULL");
				if (hasDefault)
					sb.append(" DEFAULT ").append(defaultValue);
			}

			// print column data type, optional length and scale, optional NOT NULL, optional DEFAULT value
			out.print(sb.toString());

			sb.setLength(0);	// clear the buffer for next column
		}
		cols.close();

		// add the primary key constraint definition
		// unfortunately some idiot defined that getPrimaryKeys()
		// returns the primary key columns sorted by column name, not
		// key sequence order.  So we have to sort ourself :(
		cols = dbmd.getPrimaryKeys(null, schema, name);
		int colKeySeq = cols.findColumn("KEY_SEQ");
		// first make a 'index' of the KEY_SEQ columns
		final java.util.SortedMap<Integer, Integer> seqIndex = new java.util.TreeMap<Integer, Integer>();
		for (i = 1; cols.next(); i++) {
			seqIndex.put(Integer.valueOf(cols.getInt(colKeySeq)), Integer.valueOf(i));
		}
		if (seqIndex.size() > 0) {
			cols.absolute(1);	// reset to first pk column row
			// terminate the previous line
			out.println(",");
			out.print("\tCONSTRAINT " + dq(cols.getString("PK_NAME")) + " PRIMARY KEY (");

			colNmIndex = cols.findColumn("COLUMN_NAME");
			final Iterator<Map.Entry<Integer, Integer>> it = seqIndex.entrySet().iterator();
			for (i = 0; it.hasNext(); i++) {
				final Map.Entry<Integer, Integer> e = it.next();
				cols.absolute(e.getValue().intValue());
				if (i > 0)
					out.print(", ");
				out.print(dq(cols.getString(colNmIndex)));
			}
			out.print(")");
		}
		cols.close();

		// add unique constraint definitions
		// we use getIndexInfo to get unique indexes, but need to exclude
		// the indexes which are generated by the system for pkey constraints
		cols = dbmd.getIndexInfo(null, schema, name, true, true);
		int colIndexNm = cols.findColumn("INDEX_NAME");
		colNmIndex = cols.findColumn("COLUMN_NAME");
		while (cols.next()) {
			final String idxname = cols.getString(colIndexNm);
			if (idxname != null && !idxname.endsWith("_pkey")) {
				out.println(",");
				out.print("\tCONSTRAINT " + dq(idxname) + " UNIQUE (" + dq(cols.getString(colNmIndex)));

				boolean next;
				while ((next = cols.next()) && idxname.equals(cols.getString(colIndexNm))) {
					out.print(", " + dq(cols.getString(colNmIndex)));
				}
				// go back one, we've gone one too far
				if (next)
					cols.previous();

				out.print(")");
			}
		}
		cols.close();

		// add foreign keys definitions
		cols = dbmd.getImportedKeys(null, schema, name);
		final int colFkNm = cols.findColumn("FK_NAME");
		final int colFkColNm = cols.findColumn("FKCOLUMN_NAME");
		final int colPkColNm = cols.findColumn("PKCOLUMN_NAME");
		colKeySeq = cols.findColumn("KEY_SEQ");
		final int colPkTblSch = cols.findColumn("PKTABLE_SCHEM");
		final int colPkTblNm = cols.findColumn("PKTABLE_NAME");
		final int colUpdRule = cols.findColumn("UPDATE_RULE");
		final int colDelRule = cols.findColumn("DELETE_RULE");
		final String onUpdate = " ON UPDATE ";
		final String onDelete = " ON DELETE ";
		final Set<String> fknames = new LinkedHashSet<String>(8);
		final Set<String> fk = new LinkedHashSet<String>(6);
		final Set<String> pk = new LinkedHashSet<String>(6);
		while (cols.next()) {
			out.println(",");
			out.print("\tCONSTRAINT " + dq(cols.getString(colFkNm)) + " FOREIGN KEY (");
			fknames.add(cols.getString(colFkNm));	// needed later on for exclusion of generating CREATE INDEX for them

			fk.clear();
			fk.add(cols.getString(colFkColNm));
			pk.clear();
			pk.add(cols.getString(colPkColNm));
			final short fkUpdRule = cols.getShort(colUpdRule);
			final short fkDelRule = cols.getShort(colDelRule);

			boolean next;
			while ((next = cols.next()) && cols.getInt(colKeySeq) != 1) {
				fk.add(cols.getString(colFkColNm));
				pk.add(cols.getString(colPkColNm));
			}
			// go back one
			if (next)
				cols.previous();

			Iterator<String> it = fk.iterator();
			for (i = 0; it.hasNext(); i++) {
				if (i > 0)
					out.print(", ");
				out.print(dq(it.next()));
			}
			out.print(") REFERENCES " + dq(cols.getString(colPkTblSch)) + "." + dq(cols.getString(colPkTblNm)) + " (");
			it = pk.iterator();
			for (i = 0; it.hasNext(); i++) {
				if (i > 0)
					out.print(", ");
				out.print(dq(it.next()));
			}
			out.print(")");

			// ON UPDATE { NO ACTION | CASCADE | RESTRICT | SET NULL | SET DEFAULT }
			switch (fkUpdRule) {
				 case DatabaseMetaData.importedKeyCascade:
					out.print(onUpdate);
					out.print("CASCADE");
					break;
				 case DatabaseMetaData.importedKeyNoAction:
					out.print(onUpdate);
					out.print("NO ACTION");
					break;
				 case DatabaseMetaData.importedKeyRestrict:
					out.print(onUpdate);
					out.print("RESTRICT");
					break;
				 case DatabaseMetaData.importedKeySetNull:
					out.print(onUpdate);
					out.print("SET NULL");
					break;
				 case DatabaseMetaData.importedKeySetDefault:
					out.print(onUpdate);
					out.print("SET DEFAULT");
					break;
			}
			// ON DELETE { NO ACTION | CASCADE | RESTRICT | SET NULL | SET DEFAULT }
			switch (fkDelRule) {
				 case DatabaseMetaData.importedKeyCascade:
					out.print(onDelete);
					out.print("CASCADE");
					break;
				 case DatabaseMetaData.importedKeyNoAction:
					out.print(onDelete);
					out.print("NO ACTION");
					break;
				 case DatabaseMetaData.importedKeyRestrict:
					out.print(onDelete);
					out.print("RESTRICT");
					break;
				 case DatabaseMetaData.importedKeySetNull:
					out.print(onDelete);
					out.print("SET NULL");
					break;
				 case DatabaseMetaData.importedKeySetDefault:
					out.print(onDelete);
					out.print("SET DEFAULT");
					break;
			}
		}
		cols.close();

		out.println();
		// end the create table statement
		if (type.equals("REMOTE TABLE")) {
			final String on_clause = fetchSysTablesQueryValue(dbmd.getConnection(), schema, name);
			out.println(") ON '" + ((on_clause != null) ? on_clause : "!!missing mapi:monetdb:// spec") + "';");
		} else
			out.println(");");

		// create the non unique indexes defined for this table
		// we use getIndexInfo to get non-unique indexes, but need to exclude
		// the indexes which are generated by the system for fkey constraints
		// (and pkey and unique constraints but those are marked as unique and not requested)
		cols = dbmd.getIndexInfo(null, schema, name, false, true);
		colIndexNm = cols.findColumn("INDEX_NAME");
		colNmIndex = cols.findColumn("COLUMN_NAME");
		final int tblNmIndex = cols.findColumn("TABLE_NAME");
		final int tblSchIndex = cols.findColumn("TABLE_SCHEM");
		final int nonUniqIndex = cols.findColumn("NON_UNIQUE");
		while (cols.next()) {
			if (cols.getBoolean(nonUniqIndex)) {
				// We only process non-unique indexes here.
				// The unique indexes are already covered as UNIQUE constraints in the CREATE TABLE above
				final String idxname = cols.getString(colIndexNm);
				// check idxname is not in the list of fknames for this table
				if (idxname != null && !fknames.contains(idxname)) {
					out.print("CREATE INDEX " + dq(idxname) + " ON " +
						dq(cols.getString(tblSchIndex)) + "." +
						dq(cols.getString(tblNmIndex)) + " (" +
						dq(cols.getString(colNmIndex)));

					boolean next;
					while ((next = cols.next()) && idxname.equals(cols.getString(colIndexNm))) {
						out.print(", " + dq(cols.getString(colNmIndex)));
					}
					// go back one
					if (next)
						cols.previous();

					out.println(");");
				}
			}
		}
		cols.close();
	}

	/**
	 * Dumps the given ResultSet as specified in the form variable.
	 *
	 * @param rs the ResultSet to dump
	 * @throws SQLException if a database error occurs
	 */
	public void dumpResultSet(final ResultSet rs) throws SQLException {
		switch (outputMode) {
			case VALUE_INSERT:
				resultSetToSQL(rs);
				break;
			case VALUE_COPY:
				resultSetToSQLDump(rs);
				break;
			case VALUE_TABLE:
				resultSetToTable(rs);
				break;
		}
	}

	public void setProperty(final int type, final int value) throws Exception {
		switch (type) {
			case TYPE_OUTPUT:
				switch (value) {
					case VALUE_INSERT:
					case VALUE_COPY:
					case VALUE_TABLE:
						outputMode = value;
						break;
					default:
						throw new Exception("Illegal value " + value + " for TYPE_OUTPUT");
				}
				break;
			default:
				throw new Exception("Illegal type " + type);
		}
	}

	public int getProperty(final int type) throws Exception {
		switch (type) {
			case TYPE_OUTPUT:
				return outputMode;
			default:
				throw new Exception("Illegal type " + type);
		}
	}

	private static final short AS_IS = 0;
	private static final short QUOTE = 1;

	/**
	 * Helper method to dump the contents of a table in SQL INSERT INTO
	 * format.
	 *
	 * @param rs the ResultSet to convert into INSERT INTO statements
	 * @param absolute if true, dumps table name prepended with schema name
	 * @throws SQLException if a database related error occurs
	 */
	private void resultSetToSQL(final ResultSet rs)
		throws SQLException
	{
		final ResultSetMetaData rsmd = rs.getMetaData();
		final int cols = rsmd.getColumnCount();
		// get for each output column whether it requires quotes around the value based on data type
		final short[] types = new short[cols +1];
		for (int i = 1; i <= cols; i++) {
			switch (rsmd.getColumnType(i)) {
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
				case Types.CLOB:
				case Types.BLOB:
				case Types.DATE:
				case Types.TIME:
				case Types.TIME_WITH_TIMEZONE:
				case Types.TIMESTAMP:
				case Types.TIMESTAMP_WITH_TIMEZONE:
					types[i] = QUOTE;
					break;
				case Types.NUMERIC:
				case Types.DECIMAL:
				case Types.BIT: // we don't use type BIT, it's here for completeness
				case Types.BOOLEAN:
				case Types.TINYINT:
				case Types.SMALLINT:
				case Types.INTEGER:
				case Types.BIGINT:
				case Types.REAL:
				case Types.FLOAT:
				case Types.DOUBLE:
					types[i] = AS_IS;
					break;
				default:
					// treat all other types (such as inet,url,json,objects) as complex types requiring quotes
					types[i] = QUOTE;
			}
		}

		final StringBuilder strbuf = new StringBuilder(1024);
		strbuf.append("INSERT INTO ");
		final String schema = rsmd.getSchemaName(1);
		if (schema != null && !schema.isEmpty())
			strbuf.append(dq(schema)).append(".");
		strbuf.append(dq(rsmd.getTableName(1))).append(" VALUES (");
		final int cmdpart = strbuf.length();

		while (rs.next()) {
			for (int i = 1; i <= cols; i++) {
				final String val = rs.getString(i);
				if (i > 1)
					strbuf.append(", ");
				if (val == null || rs.wasNull()) {
					strbuf.append("NULL");
				} else {
					strbuf.append((types[i] == QUOTE) ? q(val) : val);
				}
			}
			strbuf.append(");");
			out.println(strbuf.toString());
			// clear the variable part of the buffer contents for next data row
			strbuf.setLength(cmdpart);
		}
	}

	public void resultSetToSQLDump(final ResultSet rs) {
		// TODO: write copy into statement
	}

	/**
	 * Helper method to write a ResultSet in a convenient table format
	 * to the output writer.
	 *
	 * @param rs the ResultSet to write out
	 * @throws SQLException if a database related error occurs
	 */
	public void resultSetToTable(final ResultSet rs) throws SQLException {
		final ResultSetMetaData md = rs.getMetaData();
		final int cols = md.getColumnCount();
		// find the optimal display widths of the columns
		final int[] width = new int[cols + 1];
		final boolean[] isSigned = new boolean[cols + 1];	// used for controlling left or right alignment of data
		for (int j = 1; j < width.length; j++) {
			final int coldisplaysize = md.getColumnDisplaySize(j);
			final int collabellength = md.getColumnLabel(j).length();
			final int maxwidth = (coldisplaysize > collabellength) ? coldisplaysize : collabellength;
			// the minimum width should be 4 to represent: "NULL"
			width[j] = (maxwidth > 4) ? maxwidth : 4;
			isSigned[j] = md.isSigned(j);
		}

		// use a buffer to construct the text lines
		final StringBuilder strbuf = new StringBuilder(1024);

		// construct the frame lines and header text
		strbuf.append('+');
		for (int j = 1; j < width.length; j++)
			strbuf.append(repeat('-', width[j] + 1)).append("-+");

		final String outsideLine = strbuf.toString();

		strbuf.setLength(0);	// clear the buffer
		strbuf.append('|');
		for (int j = 1; j < width.length; j++) {
			final String colLabel = md.getColumnLabel(j);
			strbuf.append(' ').append(colLabel);
			strbuf.append(repeat(' ', width[j] - colLabel.length()));
			strbuf.append(" |");
		}
		// print the header text
		out.println(outsideLine);
		out.println(strbuf.toString());
		out.println(outsideLine.replace('-', '='));

		// print formatted data of each row from resultset
		long count = 0;
		for (; rs.next(); count++) {
			strbuf.setLength(0);	// clear the buffer
			strbuf.append('|');
			for (int j = 1; j < width.length; j++) {
				String data = rs.getString(j);
				if (data == null || rs.wasNull()) {
					data = "NULL";
				}

				int filler_length = width[j] - data.length();
				if (filler_length <= 0) {
					if (filler_length == 0) {
						strbuf.append(' ');
					}
					strbuf.append(data);
				} else {
					strbuf.append(' ');
					if (isSigned[j]) {
						// we have a numeric type here, right align
						strbuf.append(repeat(' ', filler_length));
						strbuf.append(data);
					} else {
						// all other left align
						strbuf.append(data);
						strbuf.append(repeat(' ', filler_length));
					}
				}
				strbuf.append(" |");
			}
			out.println(strbuf.toString());
		}

		// print the footer text
		out.println(outsideLine);
		out.print(count);
		out.println((count != 1) ? " rows" : " row");
	}

	private void changeSchema(final String schema) {
		if (lastSchema == null) {
			lastSchema = new Stack<String>();
			lastSchema.push(null);
		}

		if (!schema.equals(lastSchema.peek())) {
			if (!lastSchema.contains(schema)) {
				// do not generate CREATE SCHEMA cmds for existing system schemas
				if (!schema.equals("sys")
				 && !schema.equals("tmp")
				 && !schema.equals("json")
				 && !schema.equals("profiler")
				 && !schema.equals("wlc")	// added in Nov2019
				 && !schema.equals("wlr")	// added in Nov2019
				 && !schema.equals("logging")	// added in Jun2020
				 && !schema.equals("bam")) {
					// create schema
					out.print("CREATE SCHEMA ");
					out.print(dq(schema));
					out.println(";\n");
				}
				lastSchema.push(schema);
			}

			out.print("SET SCHEMA ");
			out.print(dq(schema));
			out.println(";\n");
		}
	}
}
