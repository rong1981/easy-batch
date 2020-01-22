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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class RandomBlockingQueueRecordWriterTest {

    private RandomBlockingQueueRecordWriter randomQueueRecordWriter;

    private BlockingQueue<Record> queue1, queue2;

    @Mock
    private Record record;

    @Before
    public void setUp() throws Exception {
        queue1 = new LinkedBlockingQueue<>();
        queue2 = new LinkedBlockingQueue<>();
        randomQueueRecordWriter = new RandomBlockingQueueRecordWriter(asList(queue1, queue2));
    }

    @Test
    public void recordsShouldBeWrittenRandomlyToOneOfTheQueues() throws Exception {
        randomQueueRecordWriter.writeRecords(new Batch(record));

        if (queue1.isEmpty()) {
            assertThat(queue2).containsOnly(record);
        } else {
            assertThat(queue2).isEmpty();
            assertThat(queue1).containsOnly(record);
        }
    }

}
