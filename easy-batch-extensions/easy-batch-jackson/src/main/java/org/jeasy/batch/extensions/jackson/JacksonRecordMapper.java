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
import org.jeasy.batch.core.mapper.RecordMapper;
import org.jeasy.batch.core.record.GenericRecord;
import org.jeasy.batch.core.record.Record;
import org.jeasy.batch.json.JsonRecord;

import static org.jeasy.batch.core.util.Utils.checkNotNull;

/**
 * Mapper that uses <a href="http://jackson.codehaus.org/">Jackson</a>
 * to map json records to domain objects.
 *
 * @param <T> Target domain object class.
 * @author Mahmoud Ben Hassine (mahmoud.benhassine@icloud.com)
 */
public class JacksonRecordMapper<T> implements RecordMapper<JsonRecord, Record<T>> {

    private ObjectMapper mapper;
    private Class<T> type;

    /**
     * Create a new {@link JacksonRecordMapper}.
     *
     * @param mapper to use
     * @param type   of target object
     */
    public JacksonRecordMapper(final ObjectMapper mapper, final Class<T> type) {
        checkNotNull(mapper, "object mapper");
        checkNotNull(type, "target type");
        this.mapper = mapper;
        this.type = type;
    }

    @Override
    public Record<T> processRecord(final JsonRecord record) throws Exception {
        return new GenericRecord<>(record.getHeader(), mapper.readValue(record.getPayload().getBytes(), type));
    }
}
