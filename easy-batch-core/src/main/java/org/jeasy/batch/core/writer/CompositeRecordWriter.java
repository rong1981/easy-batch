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

import java.util.List;

/**
 * Composite writer that delegates record writing to a list of writers.
 *
 * @author Mahmoud Ben Hassine (mahmoud.benhassine@icloud.com)
 */
public class CompositeRecordWriter implements RecordWriter {

    private List<RecordWriter> writers;

    /**
     * Create a new {@link CompositeRecordWriter}.
     *
     * @param writers delegates
     */
    public CompositeRecordWriter(List<RecordWriter> writers) {
        this.writers = writers;
    }

    @Override
    public void open() throws Exception {
        for (RecordWriter writer : writers) {
            writer.open();
        }
    }

    @Override
    public void writeRecords(Batch batch) throws Exception {
        for (RecordWriter writer : writers) {
            writer.writeRecords(batch);
        }
    }

    @Override
    public void close() throws Exception {
        for (RecordWriter writer : writers) {
            writer.close();
        }
    }
}
