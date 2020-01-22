/**
 * The MIT License
 *
 *   Copyright (c) 2020, Mahmoud Ben Hassine (mahmoud.benhassine@icloud.com)
 *
 *   Permission is hereby granted, free of charge, to any person obtaining a copy
 *   of this software and associated documentation files (the "Software"), to deal
 *   in the Software without restriction, including without limitation the rights
 *   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *   copies of the Software, and to permit persons to whom the Software is
 *   furnished to do so, subject to the following conditions:
 *
 *   The above copyright notice and this permission notice shall be included in
 *   all copies or substantial portions of the Software.
 *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 *   THE SOFTWARE.
 */
package org.jeasy.batch.extensions.integration;

import org.jeasy.batch.core.record.Batch;
import org.jeasy.batch.core.record.Record;
import org.jeasy.batch.core.writer.RecordWriter;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Write records to a list of {@link BlockingQueue}s in round-robin fashion.
 *
 * @author Mahmoud Ben Hassine (mahmoud.benhassine@icloud.com)
 */
public class RoundRobinBlockingQueueRecordWriter implements RecordWriter {

    private int queuesNumber;
    private int nextQueue;
    private List<BlockingQueue<Record>> queues;

    /**
     * Create a new {@link RoundRobinBlockingQueueRecordWriter}.
     *
     * @param queues to which records should be written
     */
    public RoundRobinBlockingQueueRecordWriter(List<BlockingQueue<Record>> queues) {
        this.queues = queues;
        this.queuesNumber = queues.size();
    }

    @Override
    public void open() {

    }

    @Override
    public void writeRecords(Batch batch) throws Exception {
        //write records to queues in round-robin fashion
        for (Record record : batch) {
            BlockingQueue<Record> queue = queues.get(nextQueue++ % queuesNumber);
            queue.put(record);
        }
    }

    @Override
    public void close() {

    }
}
