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
package org.jeasy.batch.extensions.spring;

import org.jeasy.batch.core.record.Header;
import org.jeasy.batch.core.record.Record;
import org.jeasy.batch.jdbc.JdbcRecord;
import org.jeasy.batch.test.common.Tweet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SpringJdbcRecordMapperTest {

    private SpringJdbcRecordMapper<Tweet> mapper;

    @Mock
    private JdbcRecord jdbcRecord;
    @Mock
    private Header header;
    @Mock
    private ResultSet payload;
    @Mock
    private ResultSetMetaData metadata;

    @Before
    public void setUp() throws Exception {
        mapper = new SpringJdbcRecordMapper<>(Tweet.class);
        when(jdbcRecord.getHeader()).thenReturn(header);
        when(jdbcRecord.getPayload()).thenReturn(payload);
    }

    @Test
    public void testMapRecord() throws Exception {
        when(payload.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnCount()).thenReturn(3);
        when(metadata.getColumnLabel(1)).thenReturn("id");
        when(metadata.getColumnLabel(2)).thenReturn("user");
        when(metadata.getColumnLabel(3)).thenReturn("message");
        when(payload.getInt(1)).thenReturn(1);
        when(payload.getString(2)).thenReturn("foo");
        when(payload.getString(3)).thenReturn("message");

        Record<Tweet> actual = mapper.processRecord(jdbcRecord);

        assertThat(actual.getHeader()).isEqualTo(header);
        assertThat(actual.getPayload()).isEqualTo(new Tweet(1, "foo", "message"));
    }
}
