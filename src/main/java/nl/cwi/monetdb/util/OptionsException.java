/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

package nl.cwi.monetdb.util;

public final class OptionsException extends Exception {
	static final long serialVersionUID = 42L;	// needed to prevent: warning: [serial] serializable class OptionsException has no definition of serialVersionUID

	public OptionsException(final String reason) {
		super(reason);
	}
}
