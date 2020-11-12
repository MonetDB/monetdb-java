/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

import java.sql.*;
import java.io.StringReader;
import java.nio.charset.Charset;

public class Test_PSlargebatchval {
	public static void main(String[] args) throws Exception {
		Connection con = DriverManager.getConnection(args[0]);
		Statement stmt = con.createStatement();
		PreparedStatement pstmt;

		// >> true: auto commit should be on
		System.out.println("0. true\t" + con.getAutoCommit());

		byte[] errorBytes = new byte[] { (byte) 0xe2, (byte) 0x80, (byte) 0xa7 };
		String errorStr = new String(errorBytes, Charset.forName("UTF-8"));
		StringBuilder repeatedErrorStr = new StringBuilder();
		for (int i = 0; i < 8170;i++) {
			repeatedErrorStr.append(errorStr);
		}
		String largeStr = repeatedErrorStr.toString();

		try {
			stmt.execute("CREATE TABLE x (c INT, a CLOB, b DOUBLE)");
			pstmt = con.prepareStatement("INSERT INTO x VALUES (?,?,?)");

			pstmt.setLong(1, 1L);
			pstmt.setString(2, largeStr);
			pstmt.setDouble(3, 1.0);
			pstmt.addBatch();
			pstmt.executeBatch();
			System.out.println("1. inserted 1 large string");

			/* test issue reported at https://www.monetdb.org/bugzilla/show_bug.cgi?id=3470 */
			pstmt.setLong(1, -2L);
			pstmt.setClob(2, new StringReader(largeStr));
			pstmt.setDouble(3, -2.0);
			pstmt.addBatch();
			pstmt.executeBatch();
			System.out.println("2. inserted 1 large clob via StringReader() object");

			Clob myClob = con.createClob();
			myClob.setString(1L, largeStr);

			pstmt.setLong(1, 123456789L);
			pstmt.setClob(2, myClob);
			pstmt.setDouble(3, 12345678901.98765);
			pstmt.addBatch();
			pstmt.executeBatch();
			System.out.println("3. inserted 1 large clob via createClob() object");

			pstmt.close();

			ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM x");
			if (rs.next())
				System.out.println(rs.getInt(1) + " rows inserted.");
			rs.close();

			stmt.execute("DROP TABLE x");
			stmt.close();
		} catch (SQLException e) {
			System.out.println("FAILED :( "+ e.getMessage());
			while ((e = e.getNextException()) != null)
				System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
		}

		con.close();
	}
}
