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
package org.jeasy.batch.xml;

import org.jeasy.batch.core.record.Header;
import org.jeasy.batch.core.record.Record;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class XmlRecordMarshallerTest {

    @Mock
    private Record<Person> record;
    @Mock
    private Header header;

    @Test
    public void testRecordMarshalling() throws Exception {
        // given
        Person person = new Person(1, "foo", "bar", null, false);
        when(record.getHeader()).thenReturn(header);
        when(record.getPayload()).thenReturn(person);
        XmlRecordMarshaller<Person> xmlRecordMarshaller = new XmlRecordMarshaller<>(Person.class);
        String expected = "<person><firstName>foo</firstName><id>1</id><lastName>bar</lastName><married>false</married></person>";

        // when
        XmlRecord actual = xmlRecordMarshaller.processRecord(record);

        // then
        assertThat(actual.getHeader()).isEqualTo(header);
        assertThat(actual.getPayload()).isXmlEqualTo(expected);
    }
}
