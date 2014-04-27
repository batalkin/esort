package ru.external.sort;

import junit.framework.Assert;
import junit.framework.TestCase;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Arrays;

/**
 * Created by kirill on 27.04.14.
 */
public class UtilsTest extends TestCase {
    public void testReadIntArray() throws Exception {
        File file = new File("test_data/block.tmp");
        Utils.writeNumbers(file, 200000);
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile("test_data/block.tmp", "r");
            int[] ints = new int[200000];
            for (int i = 0; i < ints.length; i++) {
                ints[i] = ints.length - i;

            }
            int[] read = new int[200000];
            int i = Utils.readIntArray(read, raf);

            Assert.assertTrue("Do not read right array", Arrays.equals(read, ints));
            Assert.assertEquals("Wrong array length returned", i, 200000);
        } finally {
            Utils.closeQuietly(raf);
        }

    }
}
