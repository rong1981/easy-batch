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

import com.univocity.parsers.common.AbstractWriter;
import com.univocity.parsers.fixed.FixedWidthWriter;
import com.univocity.parsers.fixed.FixedWidthWriterSettings;

import java.beans.IntrospectionException;

/**
 * Marshals a POJO to fixed width format using  <a href="http://www.univocity.com/">uniVocity</a>.
 *
 * <strong>This marshaller does not support recursive marshalling.</strong>
 *
 * @author Anthony Bruno (anthony.bruno196@gmail.com)
 */
public class UnivocityFixedWidthRecordMarshaller<P> extends AbstractUnivocityRecordMarshaller<P, FixedWidthWriterSettings> {

    /**
     * Create a new {@link UnivocityFixedWidthRecordMarshaller}.
     *
     * @param recordClass the type of object to marshal
     * @param settings    settings used to configure the writer object
     * @param fields      the list of fields to marshal in order
     * @throws IntrospectionException If the object to marshal cannot be introspected
     */
    public UnivocityFixedWidthRecordMarshaller(Class<P> recordClass, FixedWidthWriterSettings settings, String... fields) throws IntrospectionException {
        super(recordClass, settings, fields);
    }

    @Override
    AbstractWriter<FixedWidthWriterSettings> getWriter() {
        return new FixedWidthWriter(stringWriter, settings);
    }
}
