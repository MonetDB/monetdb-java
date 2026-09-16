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
package org.monetdb.mcl.net;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@Tag("api")
class TargetTest {

	@Test
	public void testDefaults() {
		Target target = new Target();

		for (Parameter parm : Parameter.values()) {
			Object expected = parm.getDefault();
			if (expected == null)
				continue;
			Object actual = target.getObject(parm);
			assertEquals(expected, actual, "For parameter " + parm);
		}
	}

	@Test
	public void testParameterForName() {
		for (Parameter parm : Parameter.values()) {
			Parameter found = Parameter.forName(parm.name);
			assertEquals(parm, found);
		}
	}

}
