package ru.external.sort;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Created by kirill on 23.04.14.
 */
public class ChunkReader {
    private final DataInput is;

    public ChunkReader(DataInput is) {
        this.is = is;
    }


    public synchronized int readChunk(int[] array) throws IOException {
        return Utils.readIntArray(array, is);
    }
}
