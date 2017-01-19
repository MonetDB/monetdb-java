/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

import java.sql.*;

public class BugResultSetMetaData_Bug_6183 {
	public static void main(String[] args) throws Exception {
		// Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");	// not needed anymore for self registering JDBC drivers
		Connection con = DriverManager.getConnection(args[0]);
		Statement stmt = con.createStatement();
		ResultSet rs = null;
		try {
			System.out.println("1. create table \"my dq_table\"");
			int ret = stmt.executeUpdate("CREATE TABLE \"my dq_table\" (\"my column\" varchar(30), col2 int, \"my, column\" int, \"my$column\" int, \"my#column\" int, \"my	tcolumn\" int, \"my	,tc column\" int, \"my\\\"column\" int)");
			System.out.println(" returned: " + ret);

			System.out.println("2. show column names of this new table via sys.columns query");
			rs = stmt.executeQuery("SELECT name, type, number from sys.columns where table_id in (select id from sys._tables where name = 'my dq_table') order by number");
			showResultAndClose(rs);

			System.out.println("3. insert 1 row of data");
			ret = stmt.executeUpdate("INSERT INTO \"my dq_table\" VALUES ('row1', 1,2,3,4,5,6,7)");
			System.out.println(" returned: " + ret);

			System.out.println("4. show full content of table \"my dq_table\"");
			rs = stmt.executeQuery("SELECT * from \"my dq_table\"");
			showResultAndClose(rs);

			System.out.println("5. show content of column \"my column\"");
			rs = stmt.executeQuery("SELECT \"my column\" from \"my dq_table\"");
			showResultAndClose(rs);

			System.out.println("6. show content of column \"my, column\"");
			rs = stmt.executeQuery("SELECT \"my, column\" from \"my dq_table\"");
			showResultAndClose(rs);

			System.out.println("7. show content of column \"my$column\"");
			rs = stmt.executeQuery("SELECT \"my$column\" from \"my dq_table\"");
			showResultAndClose(rs);

			System.out.println("8. show content of column \"my#column\"");
			rs = stmt.executeQuery("SELECT \"my#column\" from \"my dq_table\"");
			showResultAndClose(rs);

			System.out.println("9. show content of column \"my	tcolumn\"");
			rs = stmt.executeQuery("SELECT \"my	tcolumn\" from \"my dq_table\"");
			showResultAndClose(rs);

			System.out.println("10. show content of column \"my	,tc column\"");
			rs = stmt.executeQuery("SELECT \"my	,tc column\" from \"my dq_table\"");
			showResultAndClose(rs);

			System.out.println("11. show content of column \"my\\\"column\"");
			rs = stmt.executeQuery("SELECT \"my\\\"column\" from \"my dq_table\"");
			showResultAndClose(rs);

			System.out.println("12. show content of all columns");
			rs = stmt.executeQuery("select col2, \"my column\", \"my, column\", \"my$column\", \"my#column\", \"my	tcolumn\", \"my	,tc column\", \"my\\\"column\" from \"my dq_table\"");
			showResultAndClose(rs);

			System.out.println("Finally drop table \"my dq_table\"");
			ret = stmt.executeUpdate("DROP TABLE \"my dq_table\" ");
			System.out.println(" returned: " + ret);

		} catch (SQLException se) {
			System.out.println("Failure occurred: " + se);
		} finally {
			if (rs != null)
				rs.close();
			stmt.close();
		}
		con.close();
	}

	private static void showResultAndClose(ResultSet rs) throws SQLException {
		ResultSetMetaData rsmd = rs.getMetaData();
		int rs_col_count = rsmd.getColumnCount();
		System.out.println("Resultset with " + rs_col_count + " columns");

		System.out.println("print column names");
		for (int col = 1; col <= rs_col_count; col++) {
			if (col > 1)
				System.out.print("\t");
			System.out.print(rsmd.getColumnName(col));
		}
		System.out.println();

		System.out.println("print column labels");
		for (int col = 1; col <= rs_col_count; col++) {
			if (col > 1)
				System.out.print("\t");
			System.out.print(rsmd.getColumnLabel(col));
		}
		System.out.println();

		System.out.println("print data rows");
		while (rs.next()) {
			for (int col = 1; col <= rs_col_count; col++) {
				if (col > 1)
					System.out.print("\t");
				System.out.print(rs.getString(col));
			}
			System.out.println();
		}
		rs.close();
		System.out.println("Completed");
		System.out.println();
	}


}
