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
package org.jeasy.batch.core.filter;

import java.util.ArrayList;
import java.util.List;

import org.jeasy.batch.core.record.Record;
import org.jeasy.batch.core.util.Utils;

/**
 * A {@link RecordFilter} that saves filtered records for later use.
 * This filter delegates record filtering to another filter.
 *
 * @author Somma Daniele
 * @author Mahmoud Ben Hassine
 */
public class FilteredRecordsCollector implements RecordFilter {

    private List<Record> filteredRecords = new ArrayList<>();
    private RecordFilter delegate;

    /**
     * Create a new {@link FilteredRecordsCollector}
     *
     * @param delegate
     *          the record filter to be used
     */
    public FilteredRecordsCollector(final RecordFilter delegate) {
        Utils.checkNotNull(delegate, "delegate record filter");
        this.delegate = delegate;
    }

    @Override
    public Record processRecord(Record record) {
        Record recordFiltered = delegate.processRecord(record);
        if (null == recordFiltered) {
            filteredRecords.add(record);
        }

        return recordFiltered;
    }

    /**
     * Get filtered records.
     *
     * @return filtered records
     */
    public List<Record> getFilteredRecords() {
        return filteredRecords;
    }

}
