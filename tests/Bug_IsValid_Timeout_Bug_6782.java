/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

import java.sql.*;

public class Bug_IsValid_Timeout_Bug_6782 {
	public static void main(String[] args) throws Exception {
		Connection con = DriverManager.getConnection(args[0]);
		Statement st = null;

		st = con.createStatement();
		st.setQueryTimeout(5);
		System.out.println("getQueryTimeout must give 5: " + st.getQueryTimeout());
		st.close();

		con.isValid(3);

		st = con.createStatement();
		System.out.println("getQueryTimeout must give 0: " + st.getQueryTimeout());

		con.isValid(3);
		System.out.println("getQueryTimeout must give 0: " + st.getQueryTimeout());
		st.close();

		st.setQueryTimeout(5);
		con.isValid(3);
		System.out.println("getQueryTimeout must give 5: " + st.getQueryTimeout());
		st.close();

		con.close();
	}
}
