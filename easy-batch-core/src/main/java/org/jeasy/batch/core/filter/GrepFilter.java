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

import org.jeasy.batch.core.util.Utils;
import org.jeasy.batch.core.record.StringRecord;

/**
 * Convenient filter that mimics the unix grep utility: it keeps records containing the given pattern
 * instead of filtering them.
 *
 * Should be used with {@link StringRecord} type. Search is based on
 * {@link String#contains(CharSequence)}.
 *
 * @author Mahmoud Ben Hassine (mahmoud.benhassine@icloud.com)
 */
public class GrepFilter implements RecordFilter<StringRecord> {

    private String pattern;

    /**
     * Create a new {@link GrepFilter}.
     *
     * @param pattern the pattern to look for
     */
    public GrepFilter(final String pattern) {
        Utils.checkNotNull(pattern, "pattern");
        this.pattern = pattern;
    }

    public StringRecord processRecord(final StringRecord record) {
        String payload = record.getPayload();
        if (!payload.contains(pattern)) {
            return null;
        }
        return record;
    }

}
