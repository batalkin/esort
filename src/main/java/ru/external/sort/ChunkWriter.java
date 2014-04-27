package ru.external.sort;

import java.io.*;

/**
 * Created by kirill on 23.04.14.
 */
public class ChunkWriter{
    private final DataOutput stream;

    public ChunkWriter(DataOutput stream) {
        this.stream = stream;
    }

    public synchronized void writeChunk(int chunk[], int off, int len) throws IOException {
        if (chunk == null) {
            throw new NullPointerException();
        } else if ((off < 0) || (off > chunk.length) || (len < 0) ||
                ((off + len) > chunk.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }
        synchronized (this) {
            for (int i = 0; i < len; i++) {
                stream.writeInt(chunk[off + i]);
            }
        }
    }
}
