package nl.cwi.monetdb.mcl.connection.helpers;

import java.nio.CharBuffer;

/**
 * Created by ferreira on 12/14/16.
 */
public class BufferReallocator {

    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private static int GetNewCapacity(CharBuffer oldBuffer) {
        int minCapacity = oldBuffer.capacity() << 1;
        int newCapacity = (oldBuffer.capacity() << 1) + 2;
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

    public static CharBuffer ReallocateBuffer(CharBuffer oldBuffer) {
        int newCapacity = GetNewCapacity(oldBuffer);
        CharBuffer newBuffer = CharBuffer.allocate(newCapacity);
        oldBuffer.flip();
        newBuffer.put(oldBuffer.array());
        return newBuffer;
    }
}
