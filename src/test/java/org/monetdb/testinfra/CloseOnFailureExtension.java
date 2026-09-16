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
package org.monetdb.testinfra;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

import java.lang.reflect.Field;

/**
 * Implementation of the {@code CloseOnFailure} annotations
 */
public class CloseOnFailureExtension implements TestWatcher {

	@Override
	public void testAborted(ExtensionContext context, Throwable cause) {
		closeAnnotatedFields(context);
		TestWatcher.super.testAborted(context, cause);
	}

	@Override
	public void testFailed(ExtensionContext context, Throwable cause) {
		closeAnnotatedFields(context);
		TestWatcher.super.testFailed(context, cause);
	}

	private void closeAnnotatedFields(ExtensionContext context) {
		Object instance = context.getRequiredTestInstance();
		for (Field field : instance.getClass().getDeclaredFields()) {
			try {
				if (!field.isAnnotationPresent(CloseOnFailure.class)) {
					continue;
				}
				field.setAccessible(true);
				Object value = field.get(instance);
				if (value == null)
					continue;

				boolean isAutoCloseable = value instanceof AutoCloseable;
				if (!isAutoCloseable)
					continue;
				((AutoCloseable) value).close();
				field.set(instance, null);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

}
