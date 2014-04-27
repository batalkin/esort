package ru.external.sort;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length == 0) {
            usage();
        }

        File file = new File(args[0]);

        if (!file.exists()) {
            System.out.printf("File \"%s\" does not exist.%n", args[0]);
            System.exit(0);
        }

        if (file.length() % 4 != 0) {
            System.err.println("File does not seem to be set of 32bit integers. Its length is not divisible by 2.");
        }

        int threads = 1;

        if (args.length > 1) {
            try {
                threads = Integer.parseInt(args[1]);
            } catch (NumberFormatException ex) {
                usage();
            }

            if (threads < 1) {
                System.err.println("Number of additional threads should be greater then 1");
                usage();
            }
        }

        ExternalSorter sorter = new ExternalSorter();
        long start = System.currentTimeMillis();
        SortResult sort = sorter.sort(file, null, threads, true);

        switch (sort) {

            case WELL_DONE:
                System.out.printf("File %s has been sorted in %d ms%n", file.getPath(), System.currentTimeMillis() - start);
                break;
            case ERROR:
                System.out.printf("File %s has not been sorted%n", file.getPath());
                break;
        }

    }

    private static void usage() {
        System.out.println("Usage: sort filename [number of additional threads > 1]");
        System.out.println("Example: sort big_data 10");
        System.exit(0);
    }
}
