package ru.external.sort;

import com.sun.org.apache.xpath.internal.SourceTree;
import junit.framework.Assert;
import junit.framework.TestCase;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by kirill on 23.04.14.
 */
public class ChunkReaderTest extends TestCase {

    public void testSort() throws Exception {
        File file = new File("test_data/numbers");
        File out = new File("test_data/numbers.tmp");

        Utils.writeNumbers(file, 300000000);
        ExternalSorter sorter = new ExternalSorter();

        long start = System.currentTimeMillis();

        SortResult sort = sorter.sort(file, out, 5, true);

        switch (sort) {

            case WELL_DONE:
                System.out.printf("File %s has been sorted in %d ms", file.getPath(), System.currentTimeMillis() - start);
                break;
            case ERROR:
                System.out.printf("File %s has not been sorted", file.getPath());
                break;
        }

        Assert.assertTrue(file.getPath()+" has not been really sorted!", Utils.isSorter(file));
        Assert.assertTrue("Temp still exists", out.exists());

    }


}