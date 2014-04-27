package ru.external.sort;

import junit.framework.TestCase;

import java.io.*;

/**
 * Created by kirill on 27.04.14.
 */
public class PerformanceTest extends TestCase {
    public void testRead() throws IOException {

        File file = new File("test_data/numbers");
        BufferedRandomAccessFile accessFile = new BufferedRandomAccessFile(1024, file, "rw");
        int[] ints = new int[200000];
        long start = System.currentTimeMillis();
        accessFile.read(ints);
        System.out.printf("Done in %d%n",System.currentTimeMillis() - start);

        accessFile.close();

        DataInputStream stream = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));

        start = System.currentTimeMillis();
        Utils.readIntArray(ints, stream);
        System.out.printf("Done in %d%n",System.currentTimeMillis()-start);

    }

    public void testWrite() throws IOException {

        File file = new File("test_data/test");
        file.deleteOnExit();
        BufferedRandomAccessFile accessFile = new BufferedRandomAccessFile(1024, file, "rw");
        long start = System.currentTimeMillis();
//        for (int i = 0; i < 800000; i++) {
//            if (i==20000) {
//             accessFile.seek(16);
//            }
//            accessFile.writeInt(i);
//        }

        System.out.printf("Done in %d%n",System.currentTimeMillis() - start);



        DataOutputStream stream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(accessFile.getFD())));

        start = System.currentTimeMillis();
        for (int i = 0; i < 800000; i++) {
            stream.writeInt(i);
        }
        System.out.printf("Done in %d%n",System.currentTimeMillis()-start);



        accessFile.close();

    }
}
