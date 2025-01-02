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

public abstract class HandshakeOption<T> {
	protected final int level;
	protected final String handshakeField;
	boolean sent = false;
	T desiredValue;

	protected HandshakeOption(int level, String handshakeField, T desiredValue) {
		if (desiredValue == null) {
			throw new IllegalArgumentException("initial value must not be null");
		}
		this.level = level;
		this.handshakeField = handshakeField;
		this.desiredValue = desiredValue;
	}

	public void set(T newValue) {
		if (newValue == null) {
			throw new IllegalArgumentException("new value must not be null");
		}
		desiredValue = newValue;
	}

	public T get() {
		return desiredValue;
	}

	public int getLevel() {
		return level;
	}

	public String getFieldName() {
		return handshakeField;
	}

	public boolean isSent() {
		return sent;
	}

	public void setSent(boolean b) {
		sent = b;
	}

	public boolean mustSend(T currentValue) {
		if (sent)
			return false;
		if (currentValue.equals(desiredValue))
			return false;
		return true;
	}

	abstract long numericValue();

	protected static class BooleanOption extends HandshakeOption<Boolean> {
		protected BooleanOption(int level, String name, Boolean initialValue) {
			super(level, name, initialValue);
		}

		@Override
		long numericValue() {
			return desiredValue ? 1 : 0;
		}
	}

	public final static class AutoCommit extends BooleanOption {
		public AutoCommit(boolean autoCommit) {
			super(1, "auto_commit",  autoCommit);
		}
	}

	public final static class ReplySize extends HandshakeOption<Integer> {
		public ReplySize(int size) {
			super(2, "reply_size", size);
		}

		@Override
		long numericValue() {
			return desiredValue;
		}
	}

	public final static class SizeHeader extends BooleanOption {
		public SizeHeader(boolean sendHeader) {
			super(3, "size_header", sendHeader);
			set(sendHeader);
		}
	}

	public final static class TimeZone extends HandshakeOption<Integer> {
		public TimeZone(int offset) {
			super(5, "time_zone", offset);
		}

		@Override
		long numericValue() {
			return desiredValue;
		}
	}
}
