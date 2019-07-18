/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

import java.sql.*;

public class BugExecuteUpdate_Bug_3350 {
	public static void main(String[] args) throws Exception {
		// Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");	// not needed anymore for self registering JDBC drivers
		final Connection con = DriverManager.getConnection(args[0]);
		con.setAutoCommit(false);	// disable auto commit, so we can roll back the transaction

		final Statement stmt = con.createStatement();
		try {
			stmt.execute("CREATE TABLE t3350 (keyword VARCHAR(30) PRIMARY KEY)");
			con.commit();

			executeDML(stmt, "INSERT INTO t3350 VALUES ('Bug_3350')"); // should insert 1 row
			executeDML(stmt, "INSERT INTO t3350 VALUES ('Bug_3350')"); // this will result in an SQLException due to PK uniqueness violation
			con.rollback();

			executeDML(stmt, "INSERT INTO t3350 VALUES ('Bug_3350')"); // should insert 1 row
			executeDML(stmt, "INSERT INTO t3350 VALUES ('1'), ('x'), ('3'), ('y')"); // should insert 4 rows
			executeDML(stmt, "DELETE FROM t3350 WHERE \"keyword\" = 'Bug_3350'"); // should delete 1 row
			executeDML(stmt, "DELETE FROM t3350 WHERE \"keyword\" = 'Bug_3350'"); // should delete 0 rows
			executeDML(stmt, "UPDATE t3350 set \"keyword\" = keyword||'_ext'"); // should update 4 rows
			executeDML(stmt, "DELETE FROM t3350"); // should delete 4 rows
			con.commit();

			stmt.execute("DROP TABLE t3350");
			con.commit();
		} catch (SQLException se) {
			System.out.println(se.getMessage());
		} finally {
			stmt.close();
		}
		con.close();
	}

	private static void executeDML(Statement st, String sql) {
		try {
			int upd_count = st.executeUpdate(sql);
			System.out.println("executeUpdate(" + sql.substring(0, 6) + " ...) returned: " + upd_count);
		} catch (SQLException se) {
			System.out.println(se.getMessage());
		}

		try {
			System.out.println("getUpdateCount() returned: " + st.getUpdateCount());
		} catch (SQLException se) {
			System.out.println(se.getMessage());
		}
	}
}
