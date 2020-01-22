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
package org.jeasy.batch.core.writer;

import org.jeasy.batch.core.record.Batch;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.inOrder;

@RunWith(MockitoJUnitRunner.class)
public class CompositeRecordWriterTest {

    @Mock
    private Batch batch;
    @Mock
    private RecordWriter recordWriter1, recordWriter2;

    private CompositeRecordWriter compositeRecordWriter;

    @Before
    public void setUp() throws Exception {
        compositeRecordWriter = new CompositeRecordWriter(asList(recordWriter1, recordWriter2));
    }

    @Test
    public void testOpen() throws Exception {
        compositeRecordWriter.open();

        InOrder inOrder = inOrder(recordWriter1, recordWriter2);
        inOrder.verify(recordWriter1).open();
        inOrder.verify(recordWriter2).open();
    }

    @Test
    public void testWriteRecords() throws Exception {
        compositeRecordWriter.writeRecords(batch);

        InOrder inOrder = inOrder(recordWriter1, recordWriter2);
        inOrder.verify(recordWriter1).writeRecords(batch);
        inOrder.verify(recordWriter2).writeRecords(batch);
    }

    @Test
    public void testClose() throws Exception {
        compositeRecordWriter.close();

        InOrder inOrder = inOrder(recordWriter1, recordWriter2);
        inOrder.verify(recordWriter1).close();
        inOrder.verify(recordWriter2).close();
    }
}
