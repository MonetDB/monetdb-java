/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

package nl.cwi.monetdb.util;

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
	 * @param catalog the catalog the object is in
	 * @param schema the schema the object is in (not null)
	 * @param name the table to describe in SQL CREATE format (not null)
	 * @throws SQLException if a database related error occurs
	 */
	public void dumpSchema(
			final DatabaseMetaData dbmd,
			final String type,
			final String catalog,
			final String schema,
			final String name)
		throws SQLException
	{
		assert dbmd != null;
		assert type != null;
		assert schema != null;
		assert name != null;

		final String fqname = (useSchema ? dq(schema) + "." : "") + dq(name);

		if (useSchema)
			changeSchema(schema);

		// handle views directly
		if (type.indexOf("VIEW") != -1) {	// for types: VIEW and SYSTEM VIEW
			final String[] types = new String[1];
			types[0] = type;
			final ResultSet tbl = dbmd.getTables(catalog, schema, name, types);
			if (tbl != null) {
				if (!tbl.next()) {
					tbl.close();
					throw new SQLException("Whoops no meta data for view " + fqname);
				}

				// This will only work for MonetDB JDBC driver
				final String remarks = tbl.getString("REMARKS");	// for MonetDB driver this contains the view definition (if no comment is set) or else the comment
				if (remarks == null) {
					out.println("-- invalid " + type + " " + fqname + ": no definition found");
				} else {
					// TODO when it does not contain the  create view ...  command, but a comment, we need to use query:
					// "select query from sys.tables where name = '" + name + "' and schema_id in (select id from sys.schemas where name = '" + schema + "')"
					out.print("CREATE " + type + " " + fqname + " AS ");
					out.println(remarks.replaceFirst("create view [^ ]+ as", "").trim());
				}
				tbl.close();
			}
			return;
		}

		out.println("CREATE " + type + " " + fqname + " (");

		// add all columns with their type, nullability and default definition
		ResultSet cols = dbmd.getColumns(catalog, schema, name, null);
		final int colNmIndex = cols.findColumn("COLUMN_NAME");
		final int colTypeNmIndex = cols.findColumn("TYPE_NAME");

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

			int ctype = cols.getInt("DATA_TYPE");
			int size = cols.getInt("COLUMN_SIZE");
			int digits = cols.getInt("DECIMAL_DIGITS");
			boolean isNotNull = cols.getInt("NULLABLE") == DatabaseMetaData.columnNoNulls;
			String defaultValue = cols.getString("COLUMN_DEF");
			boolean hasDefault = (defaultValue != null && !defaultValue.isEmpty());

			s = cols.getString(colTypeNmIndex).toUpperCase();
			// do some data type substitutions to match SQL standard
			if (s.equals("INT")) {
				s = "INTEGER";
			} else if (s.equals("SEC_INTERVAL")) {
				s = "INTERVAL SECOND";
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
				case Types.TIMESTAMP:
					if (size > 1)
						sb.append('(').append(size -1).append(')');
					if (digits != 0)
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
			if (isNotNull || hasDefault) {
				final int spaces = typewidth - sb.length();
				if (spaces > 0)
					sb.append(repeat(' ', spaces));
				if (isNotNull)
					sb.append(" NOT NULL");
				if (hasDefault)
					sb.append(" DEFAULT ").append(defaultValue);
			}

			// print column type, optional length and scale, optional Not NULL, optional default value
			out.print(sb.toString());

			sb.delete(0, sb.length());	// clear the stringbuffer for next column
		}
		cols.close();

		// add the primary key constraint definition
		// unfortunately some idiot defined that getPrimaryKeys()
		// returns the primary key columns sorted by column name, not
		// key sequence order.  So we have to sort ourself :(
		cols = dbmd.getPrimaryKeys(catalog, schema, name);
		// first make an 'index' of the KEY_SEQ column
		final java.util.SortedMap<Integer, Integer> seqIndex = new java.util.TreeMap<Integer, Integer>();
		for (i = 1; cols.next(); i++) {
			seqIndex.put(Integer.valueOf(cols.getInt("KEY_SEQ")), Integer.valueOf(i));
		}
		if (seqIndex.size() > 0) {
			// terminate the previous line
			out.println(",");
			cols.absolute(1);
			out.print("\tCONSTRAINT " + dq(cols.getString("PK_NAME")) + " PRIMARY KEY (");

			final Iterator<Map.Entry<Integer, Integer>> it = seqIndex.entrySet().iterator(); 
			for (i = 0; it.hasNext(); i++) {
				final Map.Entry<Integer, Integer> e = it.next();
				cols.absolute(e.getValue().intValue());
				if (i > 0)
					out.print(", ");
				out.print(dq(cols.getString("COLUMN_NAME")));
			}
			out.print(")");
		}
		cols.close();

		// add unique constraint definitions
		// we use getIndexInfo to get unique indexes, but need to exclude
		// the indexes which are generated by the system for pkey constraints
		cols = dbmd.getIndexInfo(catalog, schema, name, true, true);
		int colIndexNm = cols.findColumn("INDEX_NAME");
		int colIndexColNm = cols.findColumn("COLUMN_NAME");
		while (cols.next()) {
			final String idxname = cols.getString(colIndexNm);
			if (idxname != null && !idxname.endsWith("_pkey")) {
				out.println(",");
				out.print("\tCONSTRAINT " + dq(idxname) + " UNIQUE (" +
					dq(cols.getString(colIndexColNm)));

				boolean next;
				while ((next = cols.next()) &&
						idxname.equals(cols.getString(colIndexNm))) {
					out.print(", " + dq(cols.getString(colIndexColNm)));
				}
				// go back one, we've gone one too far
				if (next)
					cols.previous();

				out.print(")");
			}
		}
		cols.close();

		// add foreign keys definitions
		cols = dbmd.getImportedKeys(catalog, schema, name);
		while (cols.next()) {
			out.println(",");
			out.print("\tCONSTRAINT " + dq(cols.getString("FK_NAME")) + " FOREIGN KEY (");

			final Set<String> fk = new LinkedHashSet<String>();
			fk.add(cols.getString("FKCOLUMN_NAME").intern());
			final Set<String> pk = new LinkedHashSet<String>();
			pk.add(cols.getString("PKCOLUMN_NAME").intern());

			boolean next;
			while ((next = cols.next()) &&
				cols.getInt("KEY_SEQ") != 1)
			{
				fk.add(cols.getString("FKCOLUMN_NAME").intern());
				pk.add(cols.getString("PKCOLUMN_NAME").intern());
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
			out.print(") REFERENCES " + dq(cols.getString("PKTABLE_SCHEM")) +
				"." + dq(cols.getString("PKTABLE_NAME")) + " (");
			it = pk.iterator();
			for (i = 0; it.hasNext(); i++) {
				if (i > 0)
					out.print(", ");
				out.print(dq(it.next()));
			}
			out.print(")");
		}
		cols.close();
		out.println();
		// end the create table statement
		out.println(");");

		// create the non unique indexes defined for this table
		// we use getIndexInfo to get non-unique indexes, but need to exclude
		// the indexes which are generated by the system for fkey constraints
		// (and pkey and unique constraints but those are marked as unique and not requested)
		cols = dbmd.getIndexInfo(catalog, schema, name, false, true);
		colIndexNm = cols.findColumn("INDEX_NAME");
		colIndexColNm = cols.findColumn("COLUMN_NAME");
		while (cols.next()) {
			if (cols.getBoolean("NON_UNIQUE")) {
				// We only process non-unique indexes here.
				// The unique indexes are already covered as UNIQUE constraints in the CREATE TABLE above
				final String idxname = cols.getString(colIndexNm);
				if (idxname != null && !idxname.endsWith("_fkey")) {
					out.print("CREATE INDEX " + dq(idxname) + " ON " +
						dq(cols.getString("TABLE_SCHEM")) + "." +
						dq(cols.getString("TABLE_NAME")) + " (" +
						dq(cols.getString(colIndexColNm)));

					boolean next;
					while ((next = cols.next()) &&
						idxname.equals(cols.getString(colIndexNm)))
					{
						out.print(", " + dq(cols.getString(colIndexColNm)));
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
				case Types.TIMESTAMP:
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
		if (useSchema) {
			final String schema = rsmd.getSchemaName(1);
			if (schema != null && !schema.isEmpty())
				strbuf.append(dq(schema)).append(".");
		}
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
			strbuf.append(repeat('-', width[j] + 1) + "-+");

		final String outsideLine = strbuf.toString();

		strbuf.setLength(0);	// clear the buffer
		strbuf.append('|');
		for (int j = 1; j < width.length; j++) {
			final String colLabel = md.getColumnLabel(j);
			strbuf.append(' ');
			strbuf.append(colLabel);
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
		out.println(count + " row" + (count != 1 ? "s" : ""));
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
