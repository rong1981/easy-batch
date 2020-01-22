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
package org.jeasy.batch.extensions.univocity;

import com.univocity.parsers.common.AbstractParser;
import com.univocity.parsers.fixed.FixedWidthParser;
import com.univocity.parsers.fixed.FixedWidthParserSettings;

/**
 * A record mapper that uses <a href="http://www.univocity.com/">uniVocity parsers</a> to map fixed width records
 * to domain objects.
 *
 * @author Anthony Bruno (anthony.bruno196@gmail.com)
 */
public class UnivocityFixedWidthRecordMapper<T> extends AbstractUnivocityRecordMapper<T, FixedWidthParserSettings> {

    /**
     * Create a new {@link UnivocityFixedWidthRecordMapper}.
     *
     * @param recordClass the target type
     * @param settings    the settings that is is used to configure the parser
     */
    public UnivocityFixedWidthRecordMapper(Class<T> recordClass, FixedWidthParserSettings settings) {
        super(recordClass, settings);
    }

    @Override
    protected AbstractParser<FixedWidthParserSettings> getParser() {
        return new FixedWidthParser(settings);
    }
}
