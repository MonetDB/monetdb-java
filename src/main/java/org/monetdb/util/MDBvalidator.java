/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

package org.monetdb.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Types;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * <pre>MonetDB Data Integrity Validator class (MDBvalidator) can
 * a) validate system tables data integrity in system schemas: sys and tmp
 *    this includes violations of:
 *		primary key uniqueness
 *		primary key column(s) not null
 *		unique constraint uniqueness
 *		foreign key referential integrity
 *		column not null
 *		column maximum length for char/varchar/clob/blob/json/url columns which have max length &gt; 0
 * b) validate user schema tables &amp; columns data integrity based on available meta data from system tables &amp; system views
 *		primary key uniqueness
 *	TODO primary key column(s) not null
 *		unique constraint uniqueness
 *		foreign key referential integrity
 *		column not null
 *		column maximum length for char/varchar/clob/blob/json/url columns which have max length &gt; 0
 *
 * More possible validations for future
 *		col char/varchar/clob/blob/json/url minimum length (some columns may not be empty, so length &gt;= 1)
 *		col with sequence (serial/bigserial/autoincrement) in range (0/1/min_value .. max_value)
 *		col value is valid in domain (date/time/timestamp/json/inet/url/uuid/...)
 *		col in list checks (some columns may have only certain values which are not stored in a table or view (eg as fk))
		SELECT * FROM sys.table_partitions WHERE "type" NOT IN (5,6,9,10);	-- 5=By Column Range (1+4), 6=By Expression Range (2+4), 9=By Column Value (1+8), 10=By Expression Value (2+8), see sql_catalog.h #define PARTITION_*.   Note table_partitions is introduced in Apr2019  "33"
 *		col conditional checks (column is not null when other column is (not) null)
		-- i.e.: either column_id or expression in sys.table_partitions must be populated
		SELECT "column_id", "expression", 'Missing either column_id or expression' AS violation, * FROM "sys"."table_partitions" WHERE "column_id" IS NULL AND "expression" IS NULL;
		SELECT "column_id", "expression", 'column_id and expression may not both be populated. One of them must be NULL' AS violation, * FROM "sys"."table_partitions" WHERE "column_id" IS NOT NULL AND "expression" IS NOT NULL;
 *</pre>
 * @author Martin van Dinther
 * @version 0.2
 */

public final class MDBvalidator {
	private static final String prg = "MDBvalidator";
	private Connection con;
	private int majorversion;
	private int minorversion;

	private boolean verbose = false;	// set it to true for tracing all generated SQL queries, see validateQuery(qry, ...)
	private boolean showValidationInfo = true;	// set it to false when no validation type header info should be written to stdout

	MDBvalidator(Connection conn) {
		con = conn;
	}

/* disabled as it should be called from JdbcClient program
	public static void main(String[] args) throws Exception {
		System.out.println(prg + " started with " + args.length + " arguments." + (args.length == 0 ? " Using default JDBC URL !" : ""));
		// parse input args: connection (JDBC_URL), check systbls (default) or user schema or user db

		String JDBC_URL = (args.length > 0) ? args[0]
						: "jdbc:monetdb://localhost:50000/demo?user=monetdb&password=monetdb&so_timeout=14000";
		if (!JDBC_URL.startsWith("jdbc:monetdb://")) {
			System.out.println("ERROR: Invalid JDBC URL. It does not start with jdbc:monetdb:");
			return;
		}

		Connection con = null;
		try {
			// make connection to target server
			con = java.sql.DriverManager.getConnection(JDBC_URL);
			System.out.println(prg + " connected to MonetDB server");
			printExceptions(con.getWarnings());

			long start_time = System.currentTimeMillis();

			validateSqlCatalogIntegrity(con);
			validateSqlNetcdfTablesIntegrity(con);
			validateSqlGeomTablesIntegrity(con);

			validateSchemaIntegrity(con, "sys");
			validateDBIntegrity(con);

			long elapsed = System.currentTimeMillis() - start_time;
			long secs = elapsed /1000;
			System.out.println("Validation completed in " + secs + "s and " + (elapsed - (secs *1000)) + "ms");
		} catch (SQLException e) {
			printExceptions(e);
		}

		// free resources
		if (con != null) {
			try { con.close(); } catch (SQLException e) { /* ignore * /  }
		}
	}
*/

	// public class methods (called from JdbcClient.java)
	public static void validateSqlCatalogIntegrity(final Connection conn, final boolean showValidationHeaderInfo) {
		final MDBvalidator mdbv = new MDBvalidator(conn);
		mdbv.showValidationInfo = showValidationHeaderInfo;
		if (mdbv.checkMonetDBVersion()) {
			mdbv.validateSchema("sys", null, sys_pkeys, sys_akeys, sys_fkeys, sys_notnull, true);
			mdbv.validateSchema("tmp", null, tmp_pkeys, tmp_akeys, tmp_fkeys, tmp_notnull, true);
		}
	}

	public static void validateSqlNetcdfTablesIntegrity(final Connection conn, final boolean showValidationHeaderInfo) {
		final MDBvalidator mdbv = new MDBvalidator(conn);
		mdbv.showValidationInfo = showValidationHeaderInfo;
		if (mdbv.checkMonetDBVersion()) {
			// determine if the 5 netcdf tables exist in the sys schema
			if (mdbv.checkTableExists("sys", "netcdf_files")
			 && mdbv.checkTableExists("sys", "netcdf_dims")
			 && mdbv.checkTableExists("sys", "netcdf_vars")
			 && mdbv.checkTableExists("sys", "netcdf_vardim")
			 && mdbv.checkTableExists("sys", "netcdf_attrs"))
				mdbv.validateSchema("sys", "netcdf", netcdf_pkeys, netcdf_akeys, netcdf_fkeys, netcdf_notnull, false);
		}
	}

	public static void validateSqlGeomTablesIntegrity(final Connection conn, final boolean showValidationHeaderInfo) {
		final MDBvalidator mdbv = new MDBvalidator(conn);
		mdbv.showValidationInfo = showValidationHeaderInfo;
		if (mdbv.checkMonetDBVersion()) {
			if (mdbv.checkTableExists("sys", "spatial_ref_sys"))	// No need to also test if view sys.geometry_columns exists
				mdbv.validateSchema("sys", "geom", geom_pkeys, geom_akeys, geom_fkeys, geom_notnull, false);
		}
	}

	public static void validateSchemaIntegrity(final Connection conn, String schema, final boolean showValidationHeaderInfo) {
		final MDBvalidator mdbv = new MDBvalidator(conn);
		mdbv.showValidationInfo = showValidationHeaderInfo;
		// the schema name may be surrounded by double quotes. If so, remove them.
		if (schema.startsWith("\"") && schema.endsWith("\"")) {
			schema = schema.substring(1, schema.length() -1);
		}
		if (mdbv.checkSchemaExists(schema))
			mdbv.validateSchema(schema, null, null, null, null, null, true);
		else
			if (showValidationHeaderInfo)
				System.out.println("Schema: " + schema + " does not exist in this database.");
	}

	public static void validateDBIntegrity(final Connection conn, final boolean showValidationHeaderInfo) {
		final MDBvalidator mdbv = new MDBvalidator(conn);
		mdbv.showValidationInfo = showValidationHeaderInfo;
		final Statement stmt = mdbv.createStatement("validateDBIntegrity()");
		if (stmt == null)
			return;

		boolean hasUserSchemas = false;
		ResultSet rs = null;
		try {
			// retrieve all non-system schemas
			rs = stmt.executeQuery("SELECT name FROM sys.schemas WHERE NOT system ORDER BY name;");
			if (rs != null) {
				// for each user schema do:
				while (rs.next()) {
					final String schema = rs.getString(1);
					if (schema != null && !schema.isEmpty()) {
						hasUserSchemas = true;
						mdbv.validateSchema(schema, null, null, null, null, null, true);
					}
				}
			}
		} catch (SQLException e) {
			printExceptions(e);
		}
		freeStmtRs(stmt, rs);

		if (showValidationHeaderInfo && !hasUserSchemas)
			System.out.println("No user schemas found in this database.");
	}

	// private object methods
	private void validateSchema(
		final String schema,
		final String group,
		final String[][] pkeys,
		final String[][] ukeys,
		final String[][] fkeys,
		final String[][] colnotnull,
		final boolean checkMaxStr)
	{
		final boolean is_system_schema = ("sys".equals(schema) || "tmp".equals(schema));

		if (pkeys != null) {
			validateUniqueness(schema, group, pkeys, "Primary Key uniqueness");
			validateNotNull(schema, group, pkeys, "Primary Key Not Null");
		} else {
			validateUniqueness(schema, true, "Primary Key uniqueness");
		}

		if (ukeys != null) {
			validateUniqueness(schema, group, ukeys, "Unique Constraint");
		} else {
			validateUniqueness(schema, false, "Unique Constraint");
		}

		if (fkeys != null) {
			validateFKs(schema, group, fkeys, "Foreign Key referential integrity");
		} else {
			validateFKs(schema, "Foreign Key referential integrity");
		}

		if (colnotnull != null) {
			validateNotNull(schema, group, colnotnull, "Not Null");
		} else {
			validateNotNull(schema, is_system_schema, "Not Null");
		}

		if (checkMaxStr)
			validateMaxCharStrLength(schema, is_system_schema, "Max Character Length");
	}

	/* validate uniqueness of primary key or uniqueness constraints based on static data array */
	private void validateUniqueness(
		final String schema,
		final String group,
		final String[][] data,
		final String checkType)
	{
		final int len = data.length;
		if (showValidationInfo)
			System.out.println("Checking " + minimumWidth(len,6) + (group != null ? " " + group : "") + " tables/keys  in schema " + schema + " for " + checkType + " violations.");

		final StringBuilder sb = new StringBuilder(256);	// reusable buffer to compose SQL validation queries
		sb.append("SELECT COUNT(*) AS duplicates, ");
		final int qry_len = sb.length();
		String tbl;
		String keycols;
		for (int i = 0; i < len; i++) {
			if (isValidVersion(data[i][2])) {
				tbl = data[i][0];
				keycols = data[i][1];
				// reuse the StringBuilder by cleaning it partial
				sb.setLength(qry_len);
				sb.append(keycols).append(" FROM ");
				if (!tbl.startsWith("(")) {	// when tbl starts with a ( it is a unioned table set which we cannot prefix with a schema name qualifier
					sb.append(schema).append('.');
				}
				sb.append(tbl)
				.append(" GROUP BY ").append(keycols)
				.append(" HAVING COUNT(*) > 1;");
				validateQuery(sb.toString(), schema, tbl, keycols, checkType);
			}
		}
	}

	/* validate uniqueness of primary key or uniqueness constraints based on dynamic retrieved system data from sys.keys */
	private void validateUniqueness(
		final String schema,
		final boolean pkey,
		final String checkType)
	{
		final Statement stmt = createStatement("validateUniqueness()");
		if (stmt == null)
			return;

		// fetch the primary or unique key info from the MonetDB system tables
		final StringBuilder sb = new StringBuilder(400);
		sb.append(" FROM sys.keys k JOIN sys.tables t ON k.table_id = t.id JOIN sys.schemas s ON t.schema_id = s.id"
				+ " WHERE k.type = ").append(pkey ? 0 : 1)	// 0 = primary keys, 1 = unique keys
			.append(" and s.name = '").append(schema).append("'");
		String qry = sb.toString();
		final int count = runCountQuery(qry);
		if (showValidationInfo)
			System.out.println("Checking " + minimumWidth(count,6) + " keys         in schema " + schema + " for " + checkType + " violations.");

		ResultSet rs = null;
		try {
			sb.setLength(0);	// empty previous usage of sb
			// fetch the primary or unique key info including columns from the MonetDB system tables
			sb.append("SELECT s.name as sch_nm, t.name as tbl_nm, k.name as key_nm, o.name as col_nm, o.nr")
			.append(" FROM sys.keys k JOIN sys.objects o ON k.id = o.id JOIN sys.tables t ON k.table_id = t.id JOIN sys.schemas s ON t.schema_id = s.id"
				+ " WHERE k.type = ").append(pkey ? 0 : 1)	// 0 = primary keys, 1 = unique keys
			.append(" and s.name = '").append(schema).append("'")
			.append(" ORDER BY t.name, k.name, o.nr;");
			qry = sb.toString();
			rs = stmt.executeQuery(qry);
			if (rs != null) {
				String sch = null, tbl, key, col;
				String prv_tbl = null, prv_key = null, keycols = null;
				sb.setLength(0);	// empty previous usage of sb
				sb.append("SELECT COUNT(*) AS duplicates, ");
				final int qry_len = sb.length();
				while (rs.next()) {
					// retrieve meta data
					sch = rs.getString(1);
					tbl = rs.getString(2);
					key = rs.getString(3);
					col = rs.getString(4);
					if (prv_tbl == null)
						prv_tbl = tbl;
					if (prv_key == null)
						prv_key = key;
					if (tbl.equals(prv_tbl) && key.equals(prv_key)) {
						if (keycols == null)
							keycols = "\"" + col + "\"";
						else
							keycols = keycols + ", \"" + col + "\"";
					} else {
						// compose validation query for the previous retrieved key columns
						// reuse the StringBuilder by cleaning it partial
						sb.setLength(qry_len);
						sb.append(keycols)
						.append(" FROM \"").append(sch).append("\".\"").append(prv_tbl).append('"')
						.append(" GROUP BY ").append(keycols)
						.append(" HAVING COUNT(*) > 1;");
						validateQuery(sb.toString(), sch, prv_tbl, keycols, checkType);
						prv_tbl = tbl;
						prv_key = key;
						keycols = "\"" + col + "\"";
					}
				}
				if (sch != null && prv_tbl != null && keycols != null) {
					// compose validation query for the last retrieved key
					// reuse the StringBuilder by cleaning it partial
					sb.setLength(qry_len);
					sb.append(keycols)
					.append(" FROM \"").append(sch).append("\".\"").append(prv_tbl).append('"')
					.append(" GROUP BY ").append(keycols)
					.append(" HAVING COUNT(*) > 1;");
					validateQuery(sb.toString(), sch, prv_tbl, keycols, checkType);
				}
			}
		} catch (SQLException e) {
			System.err.println("Failed to execute query: " + qry);
			printExceptions(e);
		}
		freeStmtRs(stmt, rs);
	}

	/* validate foreign key constraints based on static data array */
	private void validateFKs(
		final String schema,
		final String group,
		final String[][] data,
		final String checkType)
	{
		final int len = data.length;
		if (showValidationInfo)
			System.out.println("Checking " + minimumWidth(len,6) + (group != null ? " " + group : "") + " foreign keys in schema " + schema + " for " + checkType + " violations.");

		final StringBuilder sb = new StringBuilder(400);	// reusable buffer to compose SQL validation queries
		sb.append("SELECT ");
		final int qry_len = sb.length();
		String tbl;
		String cols;
		String ref_tbl;
		String ref_cols;
		for (int i = 0; i < len; i++) {
			if (isValidVersion(data[i][4])) {
				tbl = data[i][0];
				cols = data[i][1];
				ref_cols = data[i][2];
				ref_tbl = data[i][3];
				// reuse the StringBuilder by cleaning it partial
				sb.setLength(qry_len);
				sb.append(cols).append(", * FROM ").append(schema).append('.').append(tbl);
				if (!tbl.contains(" WHERE "))
					sb.append(" WHERE ");
				sb.append('(').append(cols).append(") NOT IN (SELECT ").append(ref_cols).append(" FROM ");
				if (!ref_tbl.contains("."))
					sb.append(schema).append('.');
				sb.append(ref_tbl).append(");");
				validateQuery(sb.toString(), schema, tbl, cols, checkType);
			}
		}
	}

	/* validate foreign key constraints based on dynamic retrieved system data from sys.keys */
	private void validateFKs(
		final String schema,
		final String checkType)
	{
		Statement stmt = null;
		try {
			// the resultset needs to be scrollable (see rs.previous())
			stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		} catch (SQLException e) {
			System.err.print("Failed to create Statement in validateFKs()");
			printExceptions(e);
		}
		if (stmt == null)
			return;

		// fetch the foreign key info from the MonetDB system tables
		final StringBuilder sb = new StringBuilder(400);
		sb.append(" FROM sys.keys k JOIN sys.tables t ON k.table_id = t.id JOIN sys.schemas s ON t.schema_id = s.id"
				+ " WHERE k.type = 2")	// 2 = foreign keys
			.append(" and s.name = '").append(schema).append("'");
		String qry = sb.toString();
		final int count = runCountQuery(qry);
		if (showValidationInfo)
			System.out.println("Checking " + minimumWidth(count,6) + " foreign keys in schema " + schema + " for " + checkType + " violations.");

		ResultSet rs = null;
		try {
			sb.setLength(0);	// empty previous usage of sb
			// fetch the foreign key columns info from the MonetDB system tables
			sb.append("SELECT " +
				"fs.name as fsch, ft.name as ftbl, fo.name as fcol, fo.nr as fnr," +
				"ps.name as psch, pt.name as ptbl, po.name as pcol" +
				// ",  fk.name as fkey, pk.name as pkey" +
				" FROM sys.keys fk" +
				" JOIN sys.objects fo ON fk.id = fo.id" +
				" JOIN sys.tables ft ON fk.table_id = ft.id" +
				" JOIN sys.schemas fs ON ft.schema_id = fs.id" +
				" JOIN sys.keys pk ON fk.rkey = pk.id" +
				" JOIN sys.objects po ON pk.id = po.id" +
				" JOIN sys.tables pt ON pk.table_id = pt.id" +
				" JOIN sys.schemas ps ON pt.schema_id = ps.id" +
				" WHERE fk.type = 2" +	// 2 = foreign keys
				" AND fo.nr = po.nr")	// important: matching fk-pk column ordering
			.append(" AND fs.name = '").append(schema).append("'")
			.append(" ORDER BY ft.name, fk.name, fo.nr;");
			qry = sb.toString();
			rs = stmt.executeQuery(qry);
			if (rs != null) {
				String fsch = null, ftbl = null, fcol = null;
				String psch = null, ptbl = null, pcol = null;
				// String fkey = null, pkey = null, 
				int fnr = -1;
				final Set<String> fk = new LinkedHashSet<String>(6);
				final Set<String> pk = new LinkedHashSet<String>(6);
				int i;
				while (rs.next()) {
					// retrieve meta data
					fsch = rs.getString(1);
					ftbl = rs.getString(2);
					fcol = rs.getString(3);
					fnr = rs.getInt(4);
					psch = rs.getString(5);
					ptbl = rs.getString(6);
					pcol = rs.getString(7);
					// fkey = rs.getString(8);
					// pkey = rs.getString(9);

					fk.clear();
					fk.add(fcol);
					pk.clear();
					pk.add(pcol);

					boolean next;
					while ((next = rs.next()) && rs.getInt(4) > 0) {
						// collect the fk and pk column names for multicolumn fks
						fk.add(rs.getString(3));
						pk.add(rs.getString(7));
					}
					// go back one
					if (next)
						rs.previous();

					// compose fk validation query for this specific fk
					// select a1, b1, * from tst.s2fk where a1 IS NOT NULL AND b1 IS NOT NULL and (a1, b1) NOT IN (select a, b from tst.s2);
					sb.setLength(0);	// empty previous usage of sb
					sb.append("SELECT ");
					Iterator<String> it = fk.iterator();
					for (i = 0; it.hasNext(); i++) {
						if (i > 0)
							sb.append(", ");
						sb.append('"').append(it.next()).append('"');
					}
					sb.append(", * FROM \"").append(fsch).append("\".\"").append(ftbl).append('"');
					sb.append(" WHERE ");
					it = fk.iterator();
					for (i = 0; it.hasNext(); i++) {
						if (i > 0)
							sb.append(" AND ");
						sb.append('"').append(it.next()).append("\" IS NOT NULL");
					}
					sb.append(" AND (");
					it = fk.iterator();
					for (i = 0; it.hasNext(); i++) {
						if (i > 0)
							sb.append(", ");
						sb.append('"').append(it.next()).append('"');
					}
					sb.append(") NOT IN (SELECT ");
					it = pk.iterator();
					for (i = 0; it.hasNext(); i++) {
						if (i > 0)
							sb.append(", ");
						sb.append('"').append(it.next()).append('"');
					}
					sb.append(" FROM \"").append(psch).append("\".\"").append(ptbl).append("\");");
					validateQuery(sb.toString(), fsch, ftbl, fcol, checkType);
				}
			}
		} catch (SQLException e) {
			System.err.println("Failed to execute query: " + qry);
			printExceptions(e);
		}
		freeStmtRs(stmt, rs);
	}

	/* validate NOT NULL constraints based on static data array */
	private void validateNotNull(
		final String schema,
		final String group,
		final String[][] data,
		final String checkType)
	{
		final int len = data.length;
		if (showValidationInfo)
			System.out.println("Checking " + minimumWidth(len,6) + (group != null ? " " + group : "") + " columns      in schema " + schema + " for " + checkType + " violations.");

		final StringBuilder sb = new StringBuilder(256);	// reusable buffer to compose SQL validation queries
		sb.append("SELECT ");
		final int qry_len = sb.length();
		String tbl;
		String col;
		boolean multicolumn = false;
		StringBuilder isNullCond = new StringBuilder(80);
		for (int i = 0; i < len; i++) {
			if (isValidVersion(data[i][2])) {
				tbl = data[i][0];
				col = data[i][1];
				multicolumn = col.contains(", ");	// some pkeys consist of multiple columns
				isNullCond.setLength(0);	// empty previous content
				if (multicolumn) {
					String[] cols = col.split(", ");
					for (int c = 0; c < cols.length; c++) {
						if (c > 0) {
							isNullCond.append(" OR ");
						}
						isNullCond.append(cols[c]).append(" IS NULL");
					}
				} else {
					isNullCond.append(col).append(" IS NULL");
				}
				// reuse the StringBuilder by cleaning it partial
				sb.setLength(qry_len);
				sb.append(col)
				.append(", * FROM ").append(schema).append('.').append(tbl)
				.append(" WHERE ").append(isNullCond).append(';');
				validateQuery(sb.toString(), schema, tbl, col, checkType);
			}
		}
	}

	/* validate NOT NULL constraints based on dynamic retrieved system data from sys.columns */
	private void validateNotNull(
		final String schema,
		final boolean system,
		final String checkType)
	{
		final Statement stmt = createStatement("validateNotNull()");
		if (stmt == null)
			return;

		// fetch the NOT NULL info from the MonetDB system tables as those are leading for user tables (but not system tables)
		final StringBuilder sb = new StringBuilder(400);
		sb.append(" from sys.columns c join sys.tables t on c.table_id = t.id join sys.schemas s on t.schema_id = s.id"
				+ " where t.type in (0, 10, 1, 11) and c.\"null\" = false"	// t.type 0 = TABLE, 10 = SYSTEM TABLE, 1 = VIEW, 11 = SYSTEM VIEW
				+ " and t.system = ").append(system)
			.append(" and s.name = '").append(schema).append("'");
		String qry = sb.toString();
		final int count = runCountQuery(qry);
		if (showValidationInfo)
			System.out.println("Checking " + minimumWidth(count,6) + " columns      in schema " + schema + " for " + checkType + " violations.");

		ResultSet rs = null;
		try {
			sb.setLength(0);	// empty previous usage of sb
			sb.append("SELECT s.name as sch_nm, t.name as tbl_nm, c.name as col_nm")	// , t.type, t.system, c.type, c.type_digits
			.append(qry).append(" ORDER BY s.name, t.name, c.name;");
			qry = sb.toString();
			rs = stmt.executeQuery(qry);
			if (rs != null) {
				String sch, tbl, col;
				while (rs.next()) {
					// retrieve meta data
					sch = rs.getString(1);
					tbl = rs.getString(2);
					col = rs.getString(3);
					// compose validation query for this specific column
					sb.setLength(0);	// empty previous usage of sb
					sb.append("SELECT '").append(sch).append('.').append(tbl).append('.').append(col).append("' as full_col_nm, *")
					.append(" FROM \"").append(sch).append("\".\"").append(tbl).append('"')
					.append(" WHERE \"").append(col).append("\" IS NULL;");
					validateQuery(sb.toString(), sch, tbl, col, checkType);
				}
			}
		} catch (SQLException e) {
			System.err.println("Failed to execute query: " + qry);
			printExceptions(e);
		}
		freeStmtRs(stmt, rs);
	}

	/* validate Maximum (Var)Char(LOB) Length constraints based on dynamic retrieved system data from sys.columns */
	private void validateMaxCharStrLength(
		final String schema,
		final boolean system,
		final String checkType)
	{
		final Statement stmt = createStatement("validateMaxCharStrLength()");
		if (stmt == null)
			return;

		// fetch the max char str len info from the MonetDB system tables as those are leading
		final StringBuilder sb = new StringBuilder(400);
		sb.append(" from sys.columns c join sys.tables t on c.table_id = t.id join sys.schemas s on t.schema_id = s.id"
				+ " where t.type in (0, 10, 1, 11)"	// t.type 0 = TABLE, 10 = SYSTEM TABLE, 1 = VIEW, 11 = SYSTEM VIEW
				+ " and c.type_digits >= 1"		// only when a positive max length is specified
				+ " and t.system = ").append(system)
			.append(" and c.type in ('varchar','char','clob','json','url','blob')")	// only for variable character/bytes data type columns
			.append(" and s.name = '").append(schema).append("'");
		String qry = sb.toString();
		final int count = runCountQuery(qry);
		if (showValidationInfo)
			System.out.println("Checking " + minimumWidth(count,6) + " columns      in schema " + schema + " for " + checkType + " violations.");

		ResultSet rs = null;
		try {
			sb.setLength(0);	// empty previous usage of sb
			sb.append("SELECT s.name as sch_nm, t.name as tbl_nm, c.name as col_nm, c.type_digits")	// , t.type, t.system, c.type
			.append(qry).append(" ORDER BY s.name, t.name, c.name, c.type_digits;");
			qry = sb.toString();
			rs = stmt.executeQuery(qry);
			if (rs != null) {
				long max_len = 0;
				String sch, tbl, col;
				while (rs.next()) {
					// retrieve meta data
					sch = rs.getString(1);
					tbl = rs.getString(2);
					col = rs.getString(3);
					max_len = rs.getLong(4);
					// patch for Aug2018 and older versions, for columns: sys._tables.query and tmp._tables.query and sys.tables.query
					if (system && max_len == 2048 && col.equals("query"))
						max_len = 1048576;
					// compose validation query for this specific column
					sb.setLength(0);	// empty previous usage of sb
					sb.append("SELECT '").append(sch).append('.').append(tbl).append('.').append(col).append("' as full_col_nm, ")
					.append(max_len).append(" as max_allowed_length, ")
					.append("length(\"").append(col).append("\") as data_length, ")
					.append('"').append(col).append("\" as data_value")
					.append(" FROM \"").append(sch).append("\".\"").append(tbl).append('"')
					.append(" WHERE \"").append(col).append("\" IS NOT NULL AND length(\"").append(col).append("\") > ").append(max_len);
					validateQuery(sb.toString(), sch, tbl, col, checkType);
				}
			}
		} catch (SQLException e) {
			System.err.println("Failed to execute query: " + qry);
			printExceptions(e);
		}
		freeStmtRs(stmt, rs);
	}


	/* Run a validation query.
	 * It should result in no rows returned.
	 * When rows are returned those are the ones that contain violations.
	 * Retrieve them and convert the results (currently first 16 only) into a (large) violation string.
	 * Log/Print the violation.
	 */
	private void validateQuery(
		final String qry,
		final String sch,
		final String tbl,
		final String cols,
		final String checkType)
	{
		final Statement stmt = createStatement("validateQuery()");
		if (stmt == null)
			return;

		ResultSet rs = null;
		try {
			if (verbose) {
				System.out.println(qry);
			}
			rs = stmt.executeQuery(qry);
			if (rs != null) {
				final ResultSetMetaData rsmd = rs.getMetaData();
				final int nr_cols = rsmd.getColumnCount();
				final StringBuilder sb = new StringBuilder(1024);
				final int maxprintrows = 16;
				int row = 0;
				String val;
				int tp;
				while (rs.next()) {
					// query returns found violations
					row++;
					if (row == 1) {
						// print result header once
						for (int i = 1; i <= nr_cols; i++) {
							sb.append((i > 1) ? ", " : "\t");
							sb.append(rsmd.getColumnLabel(i));
						}
						sb.append('\n');
					}
					if (row <= maxprintrows) { // print only the first n rows
						// retrieve row data
						for (int i = 1; i <= nr_cols; i++) {
							sb.append((i > 1) ? ", " : "\t");
							val = rs.getString(i);
							if (val == null || rs.wasNull()) {
								sb.append("null");
							} else {
								tp = rsmd.getColumnType(i);	// this method is very fast, so no need to cache it outside the loop
								if (tp == Types.VARCHAR || tp == Types.CHAR || tp == Types.CLOB
								 || tp == Types.VARBINARY || tp == Types.BLOB
								 || tp == Types.DATE || tp == Types.TIME || tp == Types.TIMESTAMP
								 || tp == Types.TIME_WITH_TIMEZONE || tp == Types.TIMESTAMP_WITH_TIMEZONE) {
									sb.append('"').append(val).append('"');
								} else {
									sb.append(val);
								}
							}
						}
						sb.append('\n');
					}
				}
				if (row > 0) {
					if (row > maxprintrows) {
						sb.append("...\n");
						sb.append("Listed only first ").append(maxprintrows).append(" violations of ").append(row).append(" found!\n");
					}
					logViolations(checkType, sch, tbl, cols, qry, sb.toString());
				}
			}
		} catch (SQLException e) {
			System.err.println("Failed to execute query: " + qry);
			printExceptions(e);
		}
		freeStmtRs(stmt, rs);
	}

	private int runCountQuery(final String from_qry) {
		final Statement stmt = createStatement("runCountQuery()");
		if (stmt == null)
			return 0;

		ResultSet rs = null;
		int count = 0;
		try {
			rs = stmt.executeQuery("SELECT COUNT(*) " + from_qry);
			if (rs != null) {
				if (rs.next()) {
					// retrieve count data
					count = rs.getInt(1);
				}
			}
		} catch (SQLException e) {
			System.err.println("Failed to execute SELECT COUNT(*) " + from_qry);
			printExceptions(e);
		}
		freeStmtRs(stmt, rs);
		return count;
	}

	private Statement createStatement(final String method) {
		try {
			return con.createStatement();
		} catch (SQLException e) {
			System.err.print("Failed to create Statement in " + method);
			printExceptions(e);
		}
		return null;
	}

	private boolean checkMonetDBVersion() {
		if (majorversion == 0 && minorversion == 0) {
			// we haven't fetched them before.
			try {
				// retrieve server version numbers (major and minor). These are needed to filter out version specific validations
				final DatabaseMetaData dbmd = con.getMetaData();
				if (dbmd != null) {
					// System.out.println("MonetDB server version " + dbmd.getDatabaseProductVersion());
					majorversion = dbmd.getDatabaseMajorVersion();
					minorversion = dbmd.getDatabaseMinorVersion();
					// check if the version number is even, if so it is an unreleased version (e.g. default branch)
					if (((minorversion  / 2 ) * 2) == minorversion) {
						// to allow testing on new tables introduced on an unreleased version, increase it with 1
						//System.out.println("Info: changed internal match version number from " + minorversion + " to " + (minorversion +1));
						minorversion++;
					}
				}
			} catch (SQLException e) {
				printExceptions(e);
			}
		}
		// validate majorversion (should be 11) and minorversion (should be >= 19) (from Jul2015 (11.19.15))
		if (majorversion < 11 || (majorversion == 11 && minorversion < 19)) {
			System.out.println("Warning: this MonetDB server is too old for " + prg + ". Please upgrade MonetDB server.");
			return false;
		}
		return true;
	}

	private boolean isValidVersion(final String version) {
		if (version == null)
			return true;	// when no version string is supplied it is valid by default

		try {
			final int v = Integer.parseInt(version);
			return minorversion >= v;
		} catch (NumberFormatException e) {
			System.out.println("Failed to parse version string '" + version + "' as an integer number.");
		}
		return false;
	}

	private boolean checkSchemaExists(final String schema) {
		final Statement stmt = createStatement("checkSchemaExists()");
		if (stmt == null)
			return false;

		final String sql = "SELECT name FROM sys.schemas WHERE name = '" + schema + "';";
		ResultSet rs = null;
		boolean ret = false;
		try {
			rs = stmt.executeQuery(sql);
			if (rs != null) {
				if (rs.next()) {
					if (schema != null && schema.equals(rs.getString(1)))
						ret = true;
				}
			}
		} catch (SQLException e) {
			System.err.println("Failed to execute " + sql);
			printExceptions(e);
		}
		freeStmtRs(stmt, rs);
		return ret;
	}

	private boolean checkTableExists(final String schema, final String table) {
		final Statement stmt = createStatement("checkTableExists()");
		if (stmt == null)
			return false;

		final String sql = "SELECT s.name, t.name FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.id WHERE t.name = '" + table + "' AND s.name = '" + schema + "';";
		ResultSet rs = null;
		boolean ret = false;
		try {
			rs = stmt.executeQuery(sql);
			if (rs != null) {
				if (rs.next()) {
					if (schema != null && schema.equals(rs.getString(1))
					 && table  != null && table.equals(rs.getString(2)) )
						ret = true;
				}
			}
		} catch (SQLException e) {
			System.err.println("Failed to execute " + sql);
			printExceptions(e);
		}
		freeStmtRs(stmt, rs);
		return ret;
	}

	private void logViolations(
		final String checkType,
		final String schema,
		final String table,
		final String columns,
		final String query,
		final String violations)
	{
		final StringBuilder sb = new StringBuilder(2048);
		sb.append(checkType).append(" violation(s) found in \"")
		  .append(schema).append("\".\"").append(table).append("\" (").append(columns).append("):\n")
		  .append(violations)
		  .append("Found using query: ").append(query).append("\n");
		System.out.println(sb.toString());
	}

	private static void printExceptions(SQLException se) {
		while (se != null) {
			System.err.println(se.getSQLState() + " " + se.getMessage());
			se = se.getNextException();
		}
	}

	private static void freeStmtRs(final Statement stmt, final ResultSet rs) {
		// free resources
		if (rs != null) {
			try { rs.close(); } catch (SQLException e) { /* ignore */ }
		}
		if (stmt != null) {
			try { stmt.close(); } catch (SQLException e) { /* ignore */ }
		}
	}

	private static String minimumWidth(int val, int minWidth) {
		final String valstr = Integer.toString(val);
		final int spacesneeded = minWidth - valstr.length();
		switch (spacesneeded) {
			case 1: return " " + valstr;
			case 2: return "  " + valstr;
			case 3: return "   " + valstr;
			case 4: return "    " + valstr;
			case 5: return "     " + valstr;
			case 6: return "      " + valstr;
			default: return valstr;
		}
	}


// ********* below are many 2-dimensional String arrays (all private) containing the data for constructing the validation queries *********
	// based on data from: https://dev.monetdb.org/hg/MonetDB/file/Jun2020/sql/test/sys-schema/Tests

	// static list of all sys tables with its pkey columns
	// each entry contains: table_nm, pk_col_nms, from_minor_version
	// data originally pulled from https://dev.monetdb.org/hg/MonetDB/file/Jun2020/sql/test/sys-schema/Tests/check_PrimaryKey_uniqueness.sql
	private static final String[][] sys_pkeys = {
		{"schemas", "id", null},
		{"_tables", "id", null},
		{"tables", "id", null},	// is a view
		{"_columns", "id", null},
		{"columns", "id", null},	// is a view
		{"functions", "id", null},
// old		{"systemfunctions", "function_id", null},	// has become a view in Apr2019 (11.33.3) and deprecated. It is removed since Jan2022 release.
		{"args", "id", null},
		{"types", "id", null},
		{"objects", "id, nr", null},
		{"keys", "id", null},
		{"idxs", "id", null},
		{"triggers", "id", null},
		{"sequences", "id", null},
		{"dependency_types", "dependency_type_id", null},
		{"dependencies", "id, depend_id", null},
		{"auths", "id", null},
		{"users", "name", null},
		{"user_role", "login_id, role_id", null},
		{"privileges", "obj_id, auth_id, privileges", null},
		{"querylog_catalog", "id", null},
		{"querylog_calls", "id", null},
		{"querylog_history", "id", null},
		{"optimizers", "name", null},
		{"environment", "name", null},	// is a view on sys.env()
		{"db_user_info", "name", null},
		{"statistics", "column_id", null},
// old	{"tracelog", "event", null},		-- Error: Profiler not started. This table now (from Jun2020) contains only: ticks, stmt
		{"\"storage\"()", "schema, table, column", null},	// the function "storage"() also lists the storage for system tables
//		{"storage", "schema, table, column", null},	// is a view on table producing function: sys.storage() which filters out all system tables.
		{"storagemodelinput", "schema, table, column", null},
//		{"storagemodel", "schema, table, column", null},	// is a view on storagemodelinput
//		{"tablestoragemodel", "schema, table", null},	// is a view on storagemodelinput

		{"rejects", "rowid", "19"},	// querying this view caused problems in versions pre Jul2015, see https://www.monetdb.org/bugzilla/show_bug.cgi?id=3794

	// new tables introduced in Jul2015 release (11.21.5)
		{"keywords", "keyword", "21"},
		{"table_types", "table_type_id", "21"},

	// new tables introduced in Jul2017 release (11.27.1)
		{"function_languages", "language_id", "27"},
		{"function_types", "function_type_id", "27"},
		{"index_types", "index_type_id", "27"},
		{"key_types", "key_type_id", "27"},
		{"privilege_codes", "privilege_code_id", "27"},

	// new tables and views introduced in Mar2018 release (11.29.3)
		{"comments", "id", "29"},
		{"ids", "id", "29"},		// is a view
		{"var_values", "var_name", "29"},	// is a view

	// new views introduced in Apr2019 feature release (11.33.3)
//		{"tablestorage", "schema, table", "33"},	// is a view on view storage, see check on "storage"() above
//		{"schemastorage", "schema", "33"},	// is a view on view storage, see check on "storage"() above
	// new tables introduced in Apr2019 feature release (11.33.3)
		{"table_partitions", "id", "33"},
		{"range_partitions", "table_id, partition_id, minimum", "33"},
		{"value_partitions", "table_id, partition_id, \"value\"", "33"},

	// changed tables in Jun2020 feature release (11.37.7)
// old	{"queue", "qtag", null},	// queue has changed in Jun2020 (11.37.7), pkey was previously qtag
		{"queue", "tag", "37"},		// queue has changed in Jun2020 (11.37.7), pkey is now called tag
// old	{"sessions", "\"user\", login, active", null},	// sessions has changed in Jun2020 (11.37.7), pkey was previously "user", login, active
		{"sessions", "sessionid", "37"},	// sessions has changed in Jun2020 (11.37.7), pkey is now called sessionid

	// new tables / views introduced in Jan2022 feature release (11.43.1)
		{"fkey_actions", "action_id", "43"},
		{"fkeys", "id", "43"}
	};

	private static final String[][] tmp_pkeys = {
		{"_tables", "id", null},
		{"_columns", "id", null},
		{"objects", "id, nr", null},
		{"keys", "id", null},
		{"idxs", "id", null},
		{"triggers", "id", null}
	};

	private static final String[][] netcdf_pkeys = {
		{"netcdf_files", "file_id", null},
		{"netcdf_attrs", "file_id, att_name", null},	// to be verified if this is correct, maybe also include obj_name
		{"netcdf_dims", "dim_id, file_id", null},
		{"netcdf_vars", "var_id, file_id", null},
		{"netcdf_vardim", "var_id, dim_id, file_id", null}
	};

	private static final String[][] geom_pkeys = {
		{"spatial_ref_sys", "srid", null}
	};


	// static list of all sys tables with its alternate key (unique constraint) columns
	// each entry contains: table_nm, ak_col_nms, from_minor_version
	// data originally pulled from https://dev.monetdb.org/hg/MonetDB/file/Jun2020/sql/test/sys-schema/Tests/check_AlternateKey_uniqueness.sql
	private static final String[][] sys_akeys = {
		{"schemas", "name", null},
		{"_tables", "schema_id, name", null},
		{"tables", "schema_id, name", null},	// is a view
		{"_columns", "table_id, name", null},
		{"columns", "table_id, name", null},	// is a view
		{"_columns", "table_id, number", null},
		{"columns", "table_id, number", null},	// is a view
		// The id values from sys.schemas, sys._tables, sys._columns and sys.functions combined must be exclusive (see FK from sys.privileges.obj_id)
		{"(SELECT id FROM sys.schemas UNION ALL SELECT id FROM sys._tables UNION ALL SELECT id FROM sys._columns UNION ALL SELECT id FROM sys.functions) as T", "T.id", null},
		{"(SELECT id FROM sys.schemas UNION ALL SELECT id FROM sys.tables UNION ALL SELECT id FROM sys.columns UNION ALL SELECT id FROM sys.functions) as T", "T.id", null},
		// the next query used to return duplicates for overloaded functions (same function but with different arg names/types), hence it has been extended
		{"functions f join sys.args a on f.id=a.func_id", "schema_id, f.name, func, mod, language, f.type, side_effect, varres, vararg, a.id", null},
		{"args", "func_id, name, inout", null},
		{"types", "schema_id, systemname, sqlname", null},
		{"objects", "id, name", null},
		{"keys", "table_id, name", null},
		{"idxs", "table_id, name", null},
		{"triggers", "table_id, name", null},
		{"sequences", "schema_id, name", null},
		{"dependency_types", "dependency_type_name", null},
		{"auths", "name", null},		// is this always unique?? is it possible to define a user and a role with the same name?
		{"optimizers", "def", null},

	// new tables introduced in older release
		{"table_types", "table_type_name", "21"},
		{"function_types", "function_type_name", "27"},
		{"function_languages", "language_name", "27"},
		{"index_types", "index_type_name", "27"},
		{"key_types", "key_type_name", "27"},
		{"privilege_codes", "privilege_code_name", "27"},
		{"comments", "id", "29"},
	// new tables introduced in Apr2019 feature release (11.33.3)
		{"table_partitions WHERE column_id IS NOT NULL", "table_id, column_id", "33"},	// requires WHERE "column_id" IS NOT NULL
		{"table_partitions WHERE \"expression\" IS NOT NULL", "table_id, \"expression\"", "33"},	// requires WHERE "expression" IS NOT NULL
		{"range_partitions", "table_id, partition_id, \"maximum\"", "33"},
	// new tables / views introduced in Jan2022 feature release (11.43.1)
		{"fkey_actions", "action_name", "43"},
		{"fkeys", "table_id, name", "43"}
	};

	private static final String[][] tmp_akeys = {
		{"_tables", "schema_id, name", null},
		{"_columns", "table_id, name", null},
		{"_columns", "table_id, number", null},
		{"objects", "id, name", null},
		{"keys", "table_id, name", null},
		{"idxs", "table_id, name", null},
		{"triggers", "table_id, name", null}
	};

	private static final String[][] netcdf_akeys = {
		{"netcdf_files", "location", null}
	};

	private static final String[][] geom_akeys = {
		{"spatial_ref_sys", "auth_name, auth_srid, srtext, proj4text", null}
	};


	// static list of all sys tables with its foreign key columns
	// each entry contains: table_nm, fk_col_nms, ref_col_nms, ref_tbl_nm, from_minor_version
	// data originally pulled from https://dev.monetdb.org/hg/MonetDB/file/Jun2020/sql/test/sys-schema/Tests/check_ForeignKey_referential_integrity.sql
	private static final String[][] sys_fkeys = {
		{"schemas", "authorization", "id", "auths", null},
		{"schemas", "owner", "id", "auths", null},
		{"_tables", "schema_id", "id", "schemas", null},
		{"tables", "schema_id", "id", "schemas", null},
		{"_tables", "type", "table_type_id", "table_types", "21"},
		{"tables", "type", "table_type_id", "table_types", "21"},
		{"_columns", "table_id", "id", "_tables", null},
		{"columns", "table_id", "id", "tables", null},
		{"_columns", "type", "sqlname", "types", null},
		{"columns", "type", "sqlname", "types", null},
		{"functions", "schema_id", "id", "schemas", null},
		{"functions", "type", "function_type_id", "function_types", "27"},
		{"functions", "language", "language_id", "function_languages", "27"},
		// system functions should refer only to functions in MonetDB system schemas
		{"functions WHERE system AND ", "schema_id", "id", "schemas WHERE system", "33"},	// column "system" was added in release 11.33.3
		{"args", "func_id", "id", "functions", null},
		{"args", "type", "sqlname", "types", null},
		{"types", "schema_id", "id", "schemas", null},
	//	{"types WHERE schema_id <> 0 AND ", "schema_id", "id", "schemas", null},	// types with schema_id = 0 should no longer exist
		{"objects", "id", "id", "ids", "29"},
		{"ids WHERE obj_type IN ('key', 'index') AND ", "id", "id", "objects", "29"},
		{"keys", "id", "id", "objects", null},
		{"keys", "table_id", "id", "_tables", null},
		{"keys", "table_id", "id", "tables", null},
		{"keys", "type", "key_type_id", "key_types", "27"},
		{"keys WHERE rkey <> -1 AND ", "rkey", "id", "keys", null},
		{"idxs", "id", "id", "objects", null},
		{"idxs", "table_id", "id", "_tables", null},
		{"idxs", "table_id", "id", "tables", null},
		{"idxs", "type", "index_type_id", "index_types", "27"},
		{"sequences", "schema_id", "id", "schemas", null},
		{"triggers", "table_id", "id", "_tables", null},
		{"triggers", "table_id", "id", "tables", null},
		{"comments", "id", "id", "ids", "29"},
		{"dependencies", "id", "id", "ids", "29"},
		{"dependencies", "depend_id", "id", "ids", "29"},
		{"dependencies", "depend_type", "dependency_type_id", "dependency_types", null},
		{"dependencies", "id, depend_id, depend_type", "v.id, v.used_by_id, v.depend_type", "dependencies_vw v", "29"},		// dependencies_vw is introduced in Mar2018 release (11.29.3), it is a view
		{"auths WHERE grantor > 0 AND ", "grantor", "id", "auths", null},
		{"users", "name", "name", "auths", null},
		{"users", "default_schema", "id", "schemas", null},
		{"db_user_info", "name", "name", "auths", null},
		{"db_user_info", "default_schema", "id", "schemas", null},
		{"user_role", "login_id", "id", "auths", null},
		{"user_role", "login_id", "a.id", "auths a WHERE a.name IN (SELECT u.name FROM sys.users u)", null},
		{"user_role", "role_id", "id", "auths", null},
		{"user_role", "role_id", "a.id", "auths a WHERE a.name IN (SELECT u.name FROM sys.users u)", null},
		{"user_role", "role_id", "id", "roles", "29"},		// roles is introduced in Mar2018 release (11.29.3), it is a view
		{"privileges", "obj_id", "id", "(SELECT id FROM sys.schemas UNION ALL SELECT id FROM sys._tables UNION ALL SELECT id FROM sys._columns UNION ALL SELECT id FROM sys.functions) as t", null},
		{"privileges", "auth_id", "id", "auths", null},
		{"privileges WHERE grantor > 0 AND ", "grantor", "id", "auths", null},
		{"privileges", "privileges", "privilege_code_id", "privilege_codes", "27"},
		{"querylog_catalog", "owner", "name", "users", null},
		{"querylog_catalog", "pipe", "name", "optimizers", null},
		{"querylog_calls", "id", "id", "querylog_catalog", null},
		{"querylog_history", "id", "id", "querylog_catalog", null},
		{"querylog_history", "owner", "name", "users", null},
		{"querylog_history", "pipe", "name", "optimizers", null},
// not a fk:	{"queue", "sessionid", "sessionid", "sessions", "37"},	// as queue contains a historical list, the session may have been closed in the meantime, so not a real persistent fk
// not a fk:	{"queue", "\"username\"", "name", "users", null},	// as queue contains a historical list, the user may have been removed in the meantime, so not a real persistent fk
		{"sessions", "\"username\"", "name", "users", "37"},
		{"sessions", "sessions.optimizer", "name", "optimizers", "37"}, 	// without the sessions. prefix it will give an error on Jun2020 release
		{"statistics", "column_id", "id", "(SELECT id FROM sys._columns UNION ALL SELECT id FROM tmp._columns) as c", null},
		{"statistics", "type", "sqlname", "types", null},
		{"storage()", "schema", "name", "schemas", null},
		{"storage()", "table", "name", "(SELECT name FROM sys._tables UNION ALL SELECT name FROM tmp._tables) as t", null},
		{"storage()", "schema, table", "sname, tname", "(SELECT sch.name as sname, tbl.name as tname FROM sys.schemas AS sch JOIN sys.tables AS tbl ON sch.id = tbl.schema_id) as t", null},
		{"storage()", "column", "name", "(SELECT name FROM sys._columns UNION ALL SELECT name FROM tmp._columns UNION ALL SELECT name FROM sys.keys UNION ALL SELECT name FROM tmp.keys UNION ALL SELECT name FROM sys.idxs UNION ALL SELECT name FROM tmp.idxs) as c", null},
		{"storage()", "type", "sqlname", "types", null},
		{"storage", "schema", "name", "schemas", null},
		{"storage", "table", "name", "(SELECT name FROM sys._tables UNION ALL SELECT name FROM tmp._tables) as t", null},
		{"storage", "schema, table", "sname, tname", "(SELECT sch.name as sname, tbl.name as tname FROM sys.schemas AS sch JOIN sys.tables AS tbl ON sch.id = tbl.schema_id) as t", null},
		{"storage", "column", "name", "(SELECT name FROM sys._columns UNION ALL SELECT name FROM tmp._columns UNION ALL SELECT name FROM sys.keys UNION ALL SELECT name FROM tmp.keys UNION ALL SELECT name FROM sys.idxs UNION ALL SELECT name FROM tmp.idxs) as c", null},
		{"storage", "type", "sqlname", "types", null},
		{"storagemodel", "schema", "name", "schemas", null},
		{"storagemodel", "table", "name", "(SELECT name FROM sys._tables UNION ALL SELECT name FROM tmp._tables) as t", null},
		{"storagemodel", "schema, table", "sname, tname", "(SELECT sch.name as sname, tbl.name as tname FROM sys.schemas AS sch JOIN sys.tables AS tbl ON sch.id = tbl.schema_id) as t", null},
		{"storagemodel", "column", "name", "(SELECT name FROM sys._columns UNION ALL SELECT name FROM tmp._columns UNION ALL SELECT name FROM sys.keys UNION ALL SELECT name FROM tmp.keys UNION ALL SELECT name FROM sys.idxs UNION ALL SELECT name FROM tmp.idxs) as c", null},
		{"storagemodel", "type", "sqlname", "types", null},
		{"storagemodelinput", "schema", "name", "schemas", null},
		{"storagemodelinput", "table", "name", "(SELECT name FROM sys._tables UNION ALL SELECT name FROM tmp._tables) as t", null},
		{"storagemodelinput", "schema, table", "sname, tname", "(SELECT sch.name as sname, tbl.name as tname FROM sys.schemas AS sch JOIN sys.tables AS tbl ON sch.id = tbl.schema_id) as t", null},
		{"storagemodelinput", "column", "name", "(SELECT name FROM sys._columns UNION ALL SELECT name FROM tmp._columns UNION ALL SELECT name FROM sys.keys UNION ALL SELECT name FROM tmp.keys UNION ALL SELECT name FROM sys.idxs UNION ALL SELECT name FROM tmp.idxs) as c", null},
		{"storagemodelinput", "type", "sqlname", "types", null},
		{"tablestoragemodel", "schema", "name", "schemas", null},
		{"tablestoragemodel", "table", "name", "(SELECT name FROM sys._tables UNION ALL SELECT name FROM tmp._tables) as t", null},
		{"tablestoragemodel", "schema, table", "sname, tname", "(SELECT sch.name as sname, tbl.name as tname FROM sys.schemas AS sch JOIN sys.tables AS tbl ON sch.id = tbl.schema_id) as t", null},
	// new tables / views introduced in Apr2019  "33"
		{"schemastorage", "schema", "name", "schemas", "33"},
		{"tablestorage", "schema", "name", "schemas", "33"},
		{"tablestorage", "table", "name", "(SELECT name FROM sys._tables UNION ALL SELECT name FROM tmp._tables) as t", "33"},
		{"tablestorage", "schema, table", "sname, tname", "(SELECT sch.name as sname, tbl.name as tname FROM sys.schemas AS sch JOIN sys.tables AS tbl ON sch.id = tbl.schema_id) as t", "33"},
		{"table_partitions", "table_id", "id", "_tables", "33"},
		{"table_partitions WHERE column_id IS NOT NULL AND ", "column_id", "id", "_columns", "33"},
		{"range_partitions", "table_id", "id", "_tables", "33"},
		{"range_partitions", "partition_id", "id", "table_partitions", "33"},
		{"value_partitions", "table_id", "id", "_tables", "33"},
		{"value_partitions", "partition_id", "id", "table_partitions", "33"},
	// new tables / views introduced in Jan2022 feature release (11.43.1)
		{"keys WHERE action >= 0 AND ", "cast(((action >> 8) & 255) as smallint)", "action_id", "fkey_actions", "43"},	// update action id
		{"keys WHERE action >= 0 AND ", "cast((action & 255) as smallint)", "action_id", "fkey_actions", "43"},	// delete action id
		{"fkeys", "id, table_id, type, name, rkey", "id, table_id, type, name, rkey", "keys", "43"},
		{"fkeys", "update_action_id", "action_id", "fkey_actions", "43"},
		{"fkeys", "delete_action_id", "action_id", "fkey_actions", "43"}
	};

	private static final String[][] tmp_fkeys = {
		{"_tables", "schema_id", "id", "sys.schemas", null},
		{"_tables", "type", "table_type_id", "sys.table_types", "21"},
		{"_columns", "table_id", "id", "_tables", null},
		{"_columns", "type", "sqlname", "sys.types", null},
		{"keys", "id", "id", "objects", null},
		{"keys", "table_id", "id", "_tables", null},
		{"keys", "type", "key_type_id", "sys.key_types", "27"},
		{"keys WHERE rkey <> -1 AND ", "rkey", "id", "keys", null},
		{"keys WHERE action >= 0 AND ", "cast(((action >> 8) & 255) as smallint)", "action_id", "sys.fkey_actions", "43"},	// update action id
		{"keys WHERE action >= 0 AND ", "cast((action & 255) as smallint)", "action_id", "sys.fkey_actions", "43"},	// delete action id
		{"idxs", "id", "id", "objects", null},
		{"idxs", "table_id", "id", "_tables", null},
		{"idxs", "type", "index_type_id", "sys.index_types", "27"},
		{"objects", "id", "id", "sys.ids", "29"},
		{"triggers", "table_id", "id", "_tables", null}
	};

	private static final String[][] netcdf_fkeys = {
		{"netcdf_attrs", "file_id", "file_id", "netcdf_files", null},
		{"netcdf_dims", "file_id", "file_id", "netcdf_files", null},
		{"netcdf_vars", "file_id", "file_id", "netcdf_files", null},
		{"netcdf_vardim", "file_id", "file_id", "netcdf_files", null},
		{"netcdf_vardim", "dim_id", "dim_id", "netcdf_dims", null},
		{"netcdf_vardim", "dim_id, file_id", "dim_id, file_id", "netcdf_dims", null},
		{"netcdf_vardim", "var_id", "var_id", "netcdf_vars", null},
		{"netcdf_vardim", "var_id, file_id", "var_id, file_id", "netcdf_vars", null}
	};

	private static final String[][] geom_fkeys = {
		{"spatial_ref_sys", "auth_srid", "srid", "spatial_ref_sys", null}
	};


	// static list of all sys tables with its not null constraint columns
	// each entry contains: table_nm, col_nm, from_minor_version
	// data originally pulled from https://dev.monetdb.org/hg/MonetDB/file/Jun2020/sql/test/sys-schema/Tests/check_Not_Nullable_columns.sql
	private static final String[][] sys_notnull = {
		{"_columns", "id", null},
		{"_columns", "name", null},
		{"_columns", "type", null},
		{"_columns", "type_digits", null},
		{"_columns", "type_scale", null},
		{"_columns", "table_id", null},
		{"_columns", "\"null\"", null},
		{"_columns", "number", null},
		{"_tables", "id", null},
		{"_tables", "name", null},
		{"_tables", "schema_id", null},
		{"_tables", "type", null},
		{"_tables", "system", null},
		{"_tables", "commit_action", null},
		{"_tables", "access", null},
		{"args", "id", null},
		{"args", "func_id", null},
		{"args", "name", null},
		{"args", "type", null},
		{"args", "type_digits", null},
		{"args", "type_scale", null},
		{"args", "inout", null},
		{"args", "number", null},
		{"auths", "id", null},
		{"auths", "name", null},
		{"auths", "grantor", null},
		{"db_user_info", "name", null},
		{"db_user_info", "fullname", null},
		{"db_user_info", "default_schema", null},
		{"dependencies", "id", null},
		{"dependencies", "depend_id", null},
		{"dependencies", "depend_type", null},
		{"function_languages", "language_id", "27"},
		{"function_languages", "language_name", "27"},
		{"function_types", "function_type_id", "27"},
		{"function_types", "function_type_name", "27"},
		{"function_types", "function_type_keyword", "29"},	// column is added in release 29
		{"functions", "id", null},
		{"functions", "name", null},
		{"functions", "func", null},
		{"functions", "mod", null},
		{"functions", "language", null},
		{"functions", "type", null},
		{"functions", "side_effect", null},
		{"functions", "varres", null},
		{"functions", "vararg", null},
		{"functions", "schema_id", null},
		{"functions", "system", "33"},
		{"idxs", "id", null},
		{"idxs", "table_id", null},
		{"idxs", "type", null},
		{"idxs", "name", null},
		{"index_types", "index_type_id", "27"},
		{"index_types", "index_type_name", "27"},
		{"key_types", "key_type_id", "27"},
		{"key_types", "key_type_name", "27"},
		{"keys", "id", null},
		{"keys", "table_id", null},
		{"keys", "type", null},
		{"keys", "name", null},
		{"keys", "rkey", null},
		{"keys", "action", null},
		{"keywords", "keyword", "21"},
		{"objects", "id", null},
		{"objects", "name", null},
		{"objects", "nr", null},
		{"optimizers", "name", null},
		{"optimizers", "def", null},
		{"optimizers", "status", null},
		{"privilege_codes", "privilege_code_id", "27"},
		{"privilege_codes", "privilege_code_name", "27"},
		{"privileges", "obj_id", null},
		{"privileges", "auth_id", null},
		{"privileges", "privileges", null},
		{"privileges", "grantor", null},
		{"privileges", "grantable", null},
		{"schemas", "id", null},
		{"schemas", "name", null},
		{"schemas", "authorization", null},
		{"schemas", "owner", null},
		{"schemas", "system", null},
		{"sequences", "id", null},
		{"sequences", "schema_id", null},
		{"sequences", "name", null},
		{"sequences", "start", null},
		{"sequences", "minvalue", null},
		{"sequences", "maxvalue", null},
		{"sequences", "increment", null},
		{"sequences", "cacheinc", null},
		{"sequences", "cycle", null},
		{"statistics", "column_id", null},
		{"statistics", "\"schema\"", "43"},	// new column as of Jan2022 release (11.43.1)
		{"statistics", "\"table\"", "43"},	// new column as of Jan2022 release (11.43.1)
		{"statistics", "\"column\"", "43"},	// new column as of Jan2022 release (11.43.1)
		{"statistics", "\"type\"", null},
		{"statistics", "\"width\"", null},
		{"statistics", "\"count\"", null},
		{"statistics", "\"unique\"", null},
		{"statistics", "nils", null},
		{"statistics", "sorted", null},
		{"statistics", "revsorted", null},
		// the table producing function "storage"() also lists the storage for system tables, whereas the view "storage" does not, so use "storage"()
		{"\"storage\"()", "schema", null},
		{"\"storage\"()", "table", null},
		{"\"storage\"()", "column", null},
		{"\"storage\"()", "type", null},
		{"\"storage\"()", "mode", null},
		{"\"storage\"()", "location", null},
		{"\"storage\"()", "count", null},
		{"\"storage\"()", "typewidth", null},
		{"\"storage\"()", "columnsize", null},
		{"\"storage\"()", "heapsize", null},
		{"\"storage\"()", "hashes", null},
		{"\"storage\"()", "phash", null},
		{"\"storage\"()", "imprints", null},
		{"\"storage\"()", "orderidx", null},
		{"storagemodelinput", "schema", null},
		{"storagemodelinput", "table", null},
		{"storagemodelinput", "column", null},
		{"storagemodelinput", "type", null},
		{"storagemodelinput", "typewidth", null},
		{"storagemodelinput", "count", null},
		{"storagemodelinput", "\"distinct\"", null},
		{"storagemodelinput", "atomwidth", null},
		{"storagemodelinput", "reference", null},
		{"storagemodelinput", "sorted", null},
		{"storagemodelinput", "\"unique\"", null},
		{"storagemodelinput", "isacolumn", "33"},
		{"table_types", "table_type_id", "21"},
		{"table_types", "table_type_name", "21"},
		{"tables", "id", null},
		{"tables", "name", null},
		{"tables", "schema_id", null},
		{"tables", "type", null},
		{"tables", "system", null},
		{"tables", "commit_action", null},
		{"tables", "access", null},
		{"tables", "temporary", null},
		{"tracelog", "ticks", null},
		{"tracelog", "stmt", null},
		{"triggers", "id", null},
		{"triggers", "name", null},
		{"triggers", "table_id", null},
		{"triggers", "time", null},
		{"triggers", "orientation", null},
		{"triggers", "event", null},
		{"triggers", "statement", null},
		{"types", "id", null},
		{"types", "systemname", null},
		{"types", "sqlname", null},
		{"types", "digits", null},
		{"types", "scale", null},
		{"types", "radix", null},
		{"types", "eclass", null},
		{"types", "schema_id", null},
		{"user_role", "login_id", null},
		{"user_role", "role_id", null},
		{"users", "name", null},
		{"users", "fullname", null},
		{"users", "default_schema", null},
		{"var_values", "var_name", "29"},
		{"var_values", "value", "29"},
	// new tables introduced in Apr2019 feature release (11.33.3)
		{"range_partitions", "table_id", "33"},
		{"range_partitions", "partition_id", "33"},
		{"range_partitions", "with_nulls", "33"},
		{"table_partitions", "id", "33"},
		{"table_partitions", "table_id", "33"},
		{"table_partitions", "type", "33"},
		{"value_partitions", "table_id", "33"},
		{"value_partitions", "partition_id", "33"},
	// new tables / views introduced in Jan2022 feature release (11.43.1)
		{"fkey_actions", "action_id", "43"},
		{"fkey_actions", "action_name", "43"},
		{"fkeys", "id", "43"},
		{"fkeys", "table_id", "43"},
		{"fkeys", "type", "43"},
		{"fkeys", "name", "43"},
		{"fkeys", "rkey", "43"},
		{"fkeys", "update_action_id", "43"},
		{"fkeys", "update_action", "43"},
		{"fkeys", "delete_action_id", "43"},
		{"fkeys", "delete_action", "43"}
	};

	private static final String[][] tmp_notnull = {
		{"_columns", "id", null},
		{"_columns", "name", null},
		{"_columns", "type", null},
		{"_columns", "type_digits", null},
		{"_columns", "type_scale", null},
		{"_columns", "table_id", null},
		{"_columns", "\"null\"", null},
		{"_columns", "number", null},
		{"_tables", "id", null},
		{"_tables", "name", null},
		{"_tables", "schema_id", null},
		{"_tables", "type", null},
		{"_tables", "system", null},
		{"_tables", "commit_action", null},
		{"_tables", "access", null},
		{"idxs", "id", null},
		{"idxs", "table_id", null},
		{"idxs", "type", null},
		{"idxs", "name", null},
		{"keys", "id", null},
		{"keys", "table_id", null},
		{"keys", "type", null},
		{"keys", "name", null},
		{"keys", "rkey", null},
		{"keys", "action", null},
		{"objects", "id", null},
		{"objects", "name", null},
		{"objects", "nr", null},
		{"triggers", "id", null},
		{"triggers", "name", null},
		{"triggers", "table_id", null},
		{"triggers", "time", null},
		{"triggers", "orientation", null},
		{"triggers", "event", null},
		{"triggers", "statement", null}
	};

	private static final String[][] netcdf_notnull = {
		{"netcdf_files", "file_id", null},
		{"netcdf_files", "location", null},
		{"netcdf_dims", "dim_id", null},
		{"netcdf_dims", "file_id", null},
		{"netcdf_dims", "name", null},
		{"netcdf_dims", "length", null},
		{"netcdf_vars", "var_id", null},
		{"netcdf_vars", "file_id", null},
		{"netcdf_vars", "name", null},
		{"netcdf_vars", "vartype", null},
		{"netcdf_vardim", "var_id", null},
		{"netcdf_vardim", "dim_id", null},
		{"netcdf_vardim", "file_id", null},
		{"netcdf_vardim", "dimpos", null},
		{"netcdf_attrs", "obj_name", null},
		{"netcdf_attrs", "att_name", null},
		{"netcdf_attrs", "att_type", null},
		{"netcdf_attrs", "value", null},
		{"netcdf_attrs", "file_id", null},
		{"netcdf_attrs", "gr_name", null}
	};

	private static final String[][] geom_notnull = {
		{"spatial_ref_sys", "srid", null},
		{"spatial_ref_sys", "auth_name", null},
		{"spatial_ref_sys", "auth_srid", null},
		{"spatial_ref_sys", "srtext", null},
		{"spatial_ref_sys", "proj4text", null}
	};
}
