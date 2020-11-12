/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

import java.sql.*;
import java.math.BigDecimal;

public class BugDecimalRound_Bug_3561 {
	public static void main(String[] args) throws Exception {
		Connection con = DriverManager.getConnection(args[0]);

		Statement stmt1 = con.createStatement();
		stmt1.executeUpdate("CREATE TABLE bug3561 (d decimal(14,4))");

		PreparedStatement st = con.prepareStatement("INSERT INTO bug3561 VALUES (?)");
		st.setBigDecimal(1, new BigDecimal("112.125"));
		st.executeUpdate();
		st.setBigDecimal(1, new BigDecimal("212.12345"));
		st.executeUpdate();
		st.setBigDecimal(1, new BigDecimal("0.012345"));
		st.executeUpdate();
		st.close();

		Statement stmt2 = con.createStatement();
		ResultSet rs = stmt2.executeQuery("SELECT d FROM bug3561");
		while (rs.next())
			System.out.println(rs.getString(1));
		rs.close();
		stmt2.close();

		stmt1.executeUpdate("DROP TABLE bug3561");
		stmt1.close();
		con.close();
	}
}
