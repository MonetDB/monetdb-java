/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.connection.helpers;

import java.nio.CharBuffer;

/**
 * An helper class to reallocate CharBuffer instance in way that won't overflow their capacity. (Adapted from the
 * {@link StringBuilder} reallocation implementation).
 *
 * @author Pedro Ferreira
 */
public final class BufferReallocator {

	/**
	 * The possible MAX_ARRAY_SIZE, according to {@link AbstractStringBuilder}.
	 */
	private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

	/**
	 * Calculates the CharBuffer's new capacity, throwing a {@link OutOfMemoryError}, if the capacity causes overflow.
	 * The capacity will always try to duplicate.
	 *
	 * @param buffer The buffer whose capacity will be expanded
	 * @return The buffer's new capacity
	 */
	private static int getNewCapacity(CharBuffer buffer) {
		int minCapacity = buffer.capacity() << 1;
		int newCapacity = (buffer.capacity() << 1) + 2;
		if (newCapacity - minCapacity < 0) {
			newCapacity = minCapacity;
		}

		if(newCapacity <= 0 || MAX_ARRAY_SIZE - newCapacity < 0) {
			if (Integer.MAX_VALUE - minCapacity < 0) { // overflow
				throw new OutOfMemoryError();
			}
			return (minCapacity > MAX_ARRAY_SIZE) ? minCapacity : MAX_ARRAY_SIZE;
		} else {
			return newCapacity;
		}
	}

	/**
	 * Reallocates the buffer by creating a new one with the new capacity and the contents of the previous one.
	 *
	 * @param buffer The buffer whose capacity will be expanded
	 * @return The new buffer allocated
	 */
	public static CharBuffer reallocateBuffer(CharBuffer buffer) {
		int newCapacity = getNewCapacity(buffer);
		CharBuffer newBuffer = CharBuffer.wrap(new char[newCapacity]);
		buffer.flip();
		newBuffer.put(buffer.array());
		return newBuffer;
	}

	/**
	 * Ensures that a buffer has a certain amount of capacity, creating a new one if the new capacity is larger than the
	 * current one in the buffer
	 *
	 * @param buffer The buffer whose capacity will be checked
	 * @param capacityThreshold The capacity threshold to test
	 * @return The original buffer or the new one allocated
	 */
	public static CharBuffer ensureCapacity(CharBuffer buffer, int capacityThreshold) {
		if(capacityThreshold > buffer.capacity()) {
			buffer = CharBuffer.wrap(new char[capacityThreshold]);
		}
		return buffer;
	}
}
