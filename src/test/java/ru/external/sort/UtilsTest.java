package ru.external.sort;

import junit.framework.Assert;
import junit.framework.TestCase;

import java.io.RandomAccessFile;
import java.util.Arrays;

/**
 * Created by kirill on 27.04.14.
 */
public class UtilsTest extends TestCase {
    public void testReadIntArray() throws Exception {
        RandomAccessFile file = null;
        try {
            file = new RandomAccessFile("test_data/block.tmp", "r");
            int[] ints = new int[200000];
            for (int i = 0; i < ints.length; i++) {
                ints[i] = ints.length - i;

            }
            int[] read = new int[200000];
            int i = Utils.readIntArray(read, file);

            Assert.assertTrue("Do not read right array", Arrays.equals(read, ints));
            Assert.assertEquals("Wrong array length returned", i, 200000);
        } finally {
            Utils.closeQuietly(file);
        }

    }
}
