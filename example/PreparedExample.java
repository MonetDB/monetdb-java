/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

import java.sql.*;

/**
 * This example shows the use of the PreparedStatement
 *
 * @author Fabian Groffen
 */
public class PreparedExample {
	public static void main(String[] args) throws Exception {
		Connection con = DriverManager.getConnection("jdbc:monetdb://localhost/notused", "monetdb", "monetdb");
		PreparedStatement st = con.prepareStatement("SELECT ? AS a1, ? AS a2");
		ResultSet rs;

		st.setString(1, "te\\s't");
		st.setInt(2, 10);

		rs = st.executeQuery();
		// get meta data and print columns with their type
		ResultSetMetaData md = rs.getMetaData();
		for (int i = 1; i <= md.getColumnCount(); i++) {
			System.out.print(md.getColumnName(i) + ":" +
				md.getColumnTypeName(i) + "\t");
		}
		System.out.println("");

		while (rs.next()) {
			for (int j = 1; j <= md.getColumnCount(); j++) {
				System.out.print(rs.getString(j) + "\t");
			}
			System.out.println("");
		}
		rs.close();

		con.close();
	}
}
