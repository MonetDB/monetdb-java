/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

import java.sql.*;

public class BugResultSetMetaData_Bug_6183 {
	static final String dqTblName = "\"my dq_table\"";
	static final String[] dqColNames = { "\"my space\"", "\"my, comma_space\"", "\"my$dollar\"", "\"my#hash\"", "\"my	tab\""
			, "\"my	,tab_comma\"", "\"my,	comma_tab\"", "\"my\"\"double_doublequote\"", "\"Abc\"", "\" \"", "\"123\"" };

	public static void main(String[] args) throws Exception {
		// Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");	// not needed anymore for self registering JDBC drivers
		Connection con = DriverManager.getConnection(args[0]);
		Statement stmt = con.createStatement();
		ResultSet rs = null;
		try {
			System.out.println("1. create table " + dqTblName);
			StringBuilder sb = new StringBuilder(30 + (dqColNames.length * (30 + 15)));
			sb.append("CREATE TABLE ").append(dqTblName).append(" (");
			for (int n = 0; n < dqColNames.length; n++) {
				sb.append(dqColNames[n]);
				sb.append(" varchar(").append(31 + n).append(')');
				if (n < (dqColNames.length -1))
					sb.append(", ");
			}
			sb.append(')');
			int ret = stmt.executeUpdate(sb.toString());
			System.out.println(" returned: " + ret + " (expected -2)");
			System.out.println();

			String tblName = dqTblName.substring(1, dqTblName.length() -1);	// trim the leading and trailing double quote characters
			System.out.println("2. show column names of this new table (" + tblName + ") via sys.columns query");
			rs = stmt.executeQuery("SELECT number, name, type from sys.columns where table_id in (select id from sys._tables where name = '" + tblName + "') order by number");
			showResultAndClose(rs);

			System.out.println("3. insert 1 row of data with values same as column names");
			sb.setLength(0);
			sb.append("INSERT INTO ").append(dqTblName).append(" VALUES (");
			for (int n = 0; n < dqColNames.length; n++) {
				sb.append('\'');
				sb.append(dqColNames[n]);
				sb.append('\'');
				if (n < (dqColNames.length -1))
					sb.append(", ");
			}
			sb.append(')');
			ret = stmt.executeUpdate(sb.toString());
			System.out.println(" returned: " + ret + " (expected 1)");
			System.out.println();

			System.out.println("4. insert 1 row of data with values same as column names but without enclosing double quotes");
			sb.setLength(0);
			sb.append("INSERT INTO ").append(dqTblName).append(" VALUES (");
			for (int n = 0; n < dqColNames.length; n++) {
				sb.append('\'');
				// remove enclosing double quotes
				sb.append(dqColNames[n].substring(1, dqColNames[n].length() -1));
				sb.append('\'');
				if (n < (dqColNames.length -1))
					sb.append(", ");
			}
			sb.append(')');
			ret = stmt.executeUpdate(sb.toString());
			System.out.println(" returned: " + ret + " (expected 1)");
			System.out.println();

			// query each column separately
			for (int n = 0; n < dqColNames.length; n++) {
				executeQueryAndShowResult(stmt, dqColNames[n], 5 + n);
			}
			// query all columns
			executeQueryAndShowResult(stmt, "*", 5 + dqColNames.length);

			System.out.println("Finally drop table " + dqTblName);
			ret = stmt.executeUpdate("DROP TABLE " + dqTblName);
			System.out.println(" returned: " + ret + " (expected -2)");
			System.out.println();
		} catch (SQLException se) {
			System.out.println("Failure occurred: " + se);
		} finally {
			if (rs != null)
				rs.close();
			stmt.close();
		}
		con.close();
	}

	private static void executeQueryAndShowResult(Statement st, String col_list, int query_count) throws SQLException {
		System.out.print(query_count);
		System.out.println(". show content of column(s): " + col_list);
		ResultSet rs = st.executeQuery("SELECT " + col_list + " from " + dqTblName);
		showResultAndClose(rs);
	}

	private static void showResultAndClose(ResultSet rs) throws SQLException {
		ResultSetMetaData rsmd = rs.getMetaData();
		int rs_col_count = rsmd.getColumnCount();
		System.out.println("Resultset with " + rs_col_count + " columns");
		System.out.println("\tColumn Name, Column Label:");
		for (int col = 1; col <= rs_col_count; col++) {
			System.out.println(col + "\t" + rsmd.getColumnName(col) + "\t" +rsmd.getColumnLabel(col));
		}

		System.out.println("Data rows:");
		long row_count = 0;
		while (rs.next()) {
			row_count++;
			for (int col = 1; col <= rs_col_count; col++) {
				if (col > 1)
					System.out.print("\t");
				System.out.print(rs.getString(col));
			}
			System.out.println();
		}
		rs.close();
		System.out.println("Listed " + row_count + " rows");
		System.out.println();
	}
}
