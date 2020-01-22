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
package org.jeasy.batch.core.processor;

import org.jeasy.batch.core.record.Record;

import java.util.ArrayList;
import java.util.List;

/**
 * Record processor that delegates processing to a pipeline of processors.
 *
 * @author Mahmoud Ben Hassine (mahmoud.benhassine@icloud.com)
 */
public class CompositeRecordProcessor implements RecordProcessor {

    private List<RecordProcessor> processors;

    /**
     * Create a new {@link CompositeRecordProcessor}.
     */
    public CompositeRecordProcessor() {
        this(new ArrayList<RecordProcessor>());
    }

    /**
     * Create a new {@link CompositeRecordProcessor}.
     *
     * @param processors delegates
     */
    public CompositeRecordProcessor(List<RecordProcessor> processors) {
        this.processors = processors;
    }

    @Override
    @SuppressWarnings(value = "unchecked")
    public Record processRecord(Record record) throws Exception {
        Record processedRecord = record;
        for (RecordProcessor processor : processors) {
            processedRecord = processor.processRecord(processedRecord);
            if (processedRecord == null) {
                return null;
            }
        }
        return processedRecord;
    }

    /**
     * Add a delegate record processor.
     *
     * @param recordProcessor to add
     */
    public void addRecordProcessor(RecordProcessor recordProcessor) {
        processors.add(recordProcessor);
    }
}
