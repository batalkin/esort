package ru.external.sort;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by kirill on 23.04.14.
 */
public class SortChunkTask implements Runnable {
    private final ChunkReader reader;
    private final int[] nums;
    private final ChunkWriter writer;

    public SortChunkTask(int[] array, ChunkReader reader, ChunkWriter writer) {
        this.reader = reader;
        this.nums = array;
        this.writer = writer;
    }


    @Override
    public void run() {
        int size;
        try {
            while ((size = reader.readChunk(nums)) != -1) {
                Arrays.sort(nums, 0, size);
                writer.writeChunk(nums, 0, size);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}