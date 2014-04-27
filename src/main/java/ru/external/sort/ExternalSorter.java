package ru.external.sort;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * Created by kirill on 23.04.14.
 */
public class ExternalSorter {

    public SortResult sort(File file, File temp, int threads, boolean verbose) throws InterruptedException, IOException {

        BufferedRandomAccessFile tempRaf = null;
        BufferedRandomAccessFile fileRaf = null;
        try {


            if ((temp = createIfAbsent(temp, verbose)) == null) {
                return SortResult.ERROR;
            }

            temp.deleteOnExit();

            int[][] blocks = new int[threads][];

            tempRaf = new BufferedRandomAccessFile(1024, temp, "rw");
            fileRaf = new BufferedRandomAccessFile(1024, file, "rw");


            int ints = (int) Math.min(file.length() / 4, 200000);

            for (int i = 0; i < threads; i++) {
                blocks[i] = new int[ints];
            }

            if (verbose) {
                System.out.printf("Sorting chunks of the base file, using %d threads.%n", threads);
            }

            boolean sortDone = sortChunks(blocks, fileRaf, tempRaf, threads);

            if (!sortDone) {
                System.err.println("Sorting process takes too long. Force termination.");
                return SortResult.ERROR;
            }

            if (verbose) {
                System.out.println("Sorting round finished");
            }


            long chunkSize = ints * 4;

            tempRaf.seek(0);
            fileRaf.seek(0);


            BufferedRandomAccessFile input = tempRaf;
            BufferedRandomAccessFile output = fileRaf;
            DataOutputStream outputStream = Utils.getBufferedStream(output);

            int round = 0;

            while (chunkSize < input.length()) {

                if (verbose) {
                    System.out.printf("Merging chunks of %d numbers%n", chunkSize / 4);
                }

                long offset = 0;
                long usedChunks;
                long maxUsedChunks = 2;

                while (offset < input.length()) {
                    usedChunks = merge(input, outputStream, offset, chunkSize, blocks);
                    offset = offset + chunkSize * usedChunks;
                    maxUsedChunks = Math.max(maxUsedChunks, usedChunks);
                }

                chunkSize *= maxUsedChunks;
                input.seek(0);
                output.seek(0);

                BufferedRandomAccessFile t = input;
                input = output;
                output = t;

                outputStream.flush();
                outputStream = Utils.getBufferedStream(output);

                round++;

            }

            if (round % 2 == 0) {
                Files.copy(temp.toPath(), file.toPath(), REPLACE_EXISTING);
            }

            return SortResult.WELL_DONE;

        } finally {
            Utils.closeQuietly(fileRaf);
            Utils.closeQuietly(tempRaf);
        }

    }

    /*    chunk_1       chunk_2        chunk_3        chunk_4
         |[block_1]....|[block_2].....|[block_3].....|[block_4].....|
          ^             ^              ^              ^
          Blocks move in their chunks.

          This method merges these chunks by loading them partially in blocks.

          [block_1]
          [block_2]
          [block_3]    => [.........] merged part in file fout.
          ...
          [block_k]

     */
    private int merge(BufferedRandomAccessFile fin, DataOutputStream fout, long offset, long chunkSize, int[][] blocks) throws IOException {

        long size1 = fin.length();

        if (offset >= size1) {
            return -1;
        }

        int chunkCount = 1;

        while (offset + chunkCount * chunkSize < size1 && chunkCount < blocks.length) chunkCount++;

        long[] chunkEnds = new long[chunkCount];
        long[] chunkPointers = new long[chunkCount];

        for (int i = 0; i < chunkCount; i++) {
            chunkPointers[i] = offset + i * chunkSize;
            chunkEnds[i] = Math.min(offset + (i + 1) * chunkSize, size1);
        }

        int[] blockSizes = new int[chunkCount];
        int[] blockStarts = new int[chunkCount];

        PriorityQueue<ValueToIndex> heap = new PriorityQueue<ValueToIndex>(blocks.length);

        for (int i = 0; i < chunkCount; i++) {
            refillBlock(i, chunkPointers, chunkEnds, blocks, blockSizes, blockStarts, heap, fin);
        }

        while (!heap.isEmpty()) {
            ValueToIndex min = heap.poll();
            fout.writeInt(min.getValue());

            if (blockStarts[min.index] + 1 < blockSizes[min.index]) {
                blockStarts[min.index] = blockStarts[min.index] + 1;
                min.setValue(blocks[min.index][blockStarts[min.index]]);
                heap.add(min);
            } else {
                chunkPointers[min.index] = chunkPointers[min.index] + blockSizes[min.index] * 4;
                refillBlock(min.index, chunkPointers, chunkEnds, blocks, blockSizes, blockStarts, heap, fin);
            }
        }

        fout.flush();

        return chunkCount;
    }

    /*

     */
    private void refillBlock(int blockIndex, long[] chunkPointers, long[] chunkEnds,
                             int[][] blocks, int[] blockSizes, int[] blockStarts, PriorityQueue<ValueToIndex> heap, BufferedRandomAccessFile input) throws IOException {
        int size = -1;
        long pointer = chunkPointers[blockIndex];
        if (pointer > -1 && pointer < chunkEnds[blockIndex]) {
            input.seek(pointer);
            size = input.read(blocks[blockIndex]);
        }

        blockSizes[blockIndex] = size;
        blockStarts[blockIndex] = 0;
        if (size > 0) {
            heap.add(new ValueToIndex(blocks[blockIndex][0], blockIndex));
        }
    }


    private boolean sortChunks(int[][] chunks, BufferedRandomAccessFile input, BufferedRandomAccessFile output, int threads) throws InterruptedException, IOException {

        input.seek(0);
        output.seek(0);

        DataOutputStream stream = Utils.getBufferedStream(output);

        ChunkReader reader = new ChunkReader(input);
        ChunkWriter writer = new ChunkWriter(stream);

        ExecutorService service = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            service.submit(new SortChunkTask(chunks[i], reader, writer));
        }
        service.shutdown();
        boolean awaitTermination = service.awaitTermination(10, TimeUnit.MINUTES);

        stream.flush();

        return awaitTermination;
    }

    private File createIfAbsent(File tempFile, boolean verbose) throws IOException {
        try {


            if (tempFile != null) {
                tempFile.delete();
                tempFile.createNewFile();
            }

            if (tempFile == null || !tempFile.exists()) {
                System.err.println("Temp file does not exist or was not set.");

                tempFile = File.createTempFile("numbers", null);
                if (verbose) {
                    System.out.println(tempFile.getAbsolutePath() + " will be used as a tempFile");
                }
            }
            return tempFile;

        } catch (IOException ex) {
            System.err.println("Can not create temp file");
            return null;
        }

    }

    private class ValueToIndex implements Comparable<ValueToIndex> {
        private int value;
        private int index;

        private ValueToIndex(int value, int index) {
            this.value = value;
            this.index = index;
        }

        public int getValue() {
            return value;
        }

        public int getIndex() {
            return index;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public void setIndex(int index) {
            this.index = index;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ValueToIndex that = (ValueToIndex) o;

            if (index != that.index) return false;
            if (value != that.value) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = value;
            result = 31 * result + index;
            return result;
        }

        @Override
        public int compareTo(ValueToIndex o) {
            return Integer.valueOf(value).compareTo(o.getValue());
        }
    }
}
