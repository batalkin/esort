package ru.external.sort;

import java.io.*;

/**
 * Created by kirill on 23.04.14.
 */
public class Utils {
    public static void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ignored) {
            }
        }
    }

    public static int readIntArray(int[] array, DataInput is) throws IOException {
        int i = 0;
        for (; i < array.length; i++) {
            try {
                array[i] = is.readInt();
            } catch (EOFException eof) {
                i = (i == 0) ? -1 : i;
                break;
            }
        }

        return i;
    }

    public static void writeNumbers(File file, int max) throws IOException {
        file.delete();
        if (!file.exists()) {
            file.createNewFile();
        }
        DataOutputStream stream = null;
        try {
            stream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
            for (int i = max; i > 0; i--) {
                stream.writeInt(i);
            }
            stream.flush();

        } finally {
            if (stream != null) {
                stream.close();
            }
        }

    }

    public static void print(File file) throws IOException {
        DataInputStream stream = null;
        try {
            stream = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
            try {
                int i = 1;
                while (true) {
                    System.out.print(stream.readInt() + " ");
                    if (i++ % 20 == 0) {
                        System.out.println();
                    }
                }
            } catch (EOFException e) {
            }
        } finally {
            closeQuietly(stream);
        }
    }

    public static boolean isSorter(File file) throws Exception {
        DataInputStream stream = null;
        try {
            stream = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
            int i = 0;
            int j = 0;
            try {
                while (true) {
                    i = stream.readInt();
                    j = stream.readInt();
                    if (j < i) {
                        return false;
                    }
                }
            } catch (EOFException e) {
                return true;
            }
        } finally {
            closeQuietly(stream);
        }
    }


    public static DataOutputStream getBufferedStream(RandomAccessFile output) throws IOException {
        return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(output.getFD())));
    }

}
