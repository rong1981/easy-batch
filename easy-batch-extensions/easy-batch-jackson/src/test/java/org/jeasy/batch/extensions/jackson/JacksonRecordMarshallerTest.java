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

import org.jeasy.batch.core.record.GenericRecord;
import org.jeasy.batch.core.record.Header;
import org.jeasy.batch.core.record.Record;
import org.jeasy.batch.json.JsonRecord;
import org.jeasy.batch.test.common.Tweet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class JacksonRecordMarshallerTest {

    @Mock
    private Header header;

    private JacksonRecordMarshaller<Tweet> marshaller;

    @Before
    public void setUp() {
        marshaller = new JacksonRecordMarshaller<>();
    }

    @Test
    public void marshal() throws Exception {
        Tweet tweet = new Tweet(1, "foo", "hi");
        Record<Tweet> record = new GenericRecord<>(header, tweet);

        String expected = "{\"id\":1,\"user\":\"foo\",\"message\":\"hi\"}";
        JsonRecord actual = marshaller.processRecord(record);

        assertThat(actual.getHeader()).isEqualTo(header);
        assertThat(actual.getPayload()).isEqualTo(expected);
    }
}
