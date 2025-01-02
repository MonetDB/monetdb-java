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

package org.monetdb.util;

public final class OptionsException extends Exception {
	static final long serialVersionUID = 42L;	// needed to prevent: warning: [serial] serializable class OptionsException has no definition of serialVersionUID

	public OptionsException(final String reason) {
		super(reason);
	}
}
