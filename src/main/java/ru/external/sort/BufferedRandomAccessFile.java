package ru.external.sort;

import java.io.*;

/**
 * Created by kirill on 27.04.14.
 */
public class BufferedRandomAccessFile extends RandomAccessFile {

    private byte[] bytebuffer;
    private int maxread;
    private int buffpos;

    public BufferedRandomAccessFile(int bufferSize, File file, String mode) throws FileNotFoundException {
        super(file, mode);
        bytebuffer = new byte[bufferSize];
        maxread = 0;
        buffpos = 0;
    }

    @Override
    public long getFilePointer() throws IOException {
        return super.getFilePointer() + buffpos;
    }

    @Override
    public void seek(long pos) throws IOException {
        if (maxread != -1 && pos < (super.getFilePointer() + maxread) && pos > super.getFilePointer()) {
            Long diff = (pos - super.getFilePointer());
            if (diff < Integer.MAX_VALUE) {
                buffpos = diff.intValue();
            } else {
                throw new IOException("Something wrong with raf buffering");
            }
        } else {
            buffpos = 0;
            super.seek(pos);
            maxread = readChunk();
        }
    }

    private int readChunk() throws IOException {
        long pos = super.getFilePointer() + buffpos;
        super.seek(pos);
        int read = super.read(bytebuffer);
        super.seek(pos);
        buffpos = 0;
        return read;
    }

    @Override
    public int read() throws IOException {
        if (buffpos >= maxread) {
            maxread = readChunk();
            if (maxread == -1) {
                return -1;
            }
        }
        buffpos++;
        return bytebuffer[buffpos - 1] & 0xFF;
    }

    public int read(int[] array) throws IOException {
        int i = 0;
        for (; i < array.length; i++) {
            try {
                array[i] = readInt();
            } catch (EOFException eof) {
                i = (i == 0) ? -1 : i;
                break;
            }
        }

        return i;
    }
}
