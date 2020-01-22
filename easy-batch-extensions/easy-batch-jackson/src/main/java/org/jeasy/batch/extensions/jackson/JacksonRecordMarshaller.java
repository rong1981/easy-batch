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
package org.jeasy.batch.extensions.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jeasy.batch.core.marshaller.RecordMarshaller;
import org.jeasy.batch.core.record.Record;
import org.jeasy.batch.json.JsonRecord;

import static org.jeasy.batch.core.util.Utils.checkNotNull;

/**
 * Marshals a POJO to Json using <a href="http://jackson.codehaus.org/">Jackson</a>.
 *
 * @param <P> Target domain object class.
 * @author Mahmoud Ben Hassine (mahmoud.benhassine@icloud.com)
 */
public class JacksonRecordMarshaller<P> implements RecordMarshaller<Record<P>, JsonRecord> {

    private ObjectMapper mapper;

    /**
     * Create a new {@link JacksonRecordMarshaller}.
     */
    public JacksonRecordMarshaller() {
        mapper = new ObjectMapper();
    }

    /**
     * Create a new {@link JacksonRecordMarshaller}.
     *
     * @param mapper a pre-configured {@link ObjectMapper} instance
     */
    public JacksonRecordMarshaller(final ObjectMapper mapper) {
        checkNotNull(mapper, "object mapper");
        this.mapper = mapper;
    }

    @Override
    public JsonRecord processRecord(final Record<P> record) throws Exception {
        return new JsonRecord(record.getHeader(), mapper.writeValueAsString(record.getPayload()));
    }

}
