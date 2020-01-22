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
package org.jeasy.batch.extensions.integration.jms;

import org.jeasy.batch.extensions.integration.DefaultPredicate;
import org.jeasy.batch.extensions.integration.Predicate;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.jms.QueueSender;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class ContentBasedJmsQueueRecordWriterBuilderTest {

    @Mock
    private QueueSender queue, defaultQueue;
    @Mock
    private Predicate predicate;

    @Test
    public void recordWriterShouldBeCorrectlyBuilt() throws Exception {
        ContentBasedJmsQueueRecordWriter recordWriter = ContentBasedJmsQueueRecordWriterBuilder.newContentBasedJmsQueueRecordWriterBuilder()
                .when(predicate)
                .writeTo(queue)
                .otherwise(defaultQueue)
                .build();

        assertThat(recordWriter).isNotNull();
        Map<Predicate, QueueSender> queueMap = recordWriter.getQueueMap();
        assertThat(queueMap).isNotNull();
        assertThat(queueMap.get(predicate)).isEqualTo(queue);
        assertThat(queueMap.get(new DefaultPredicate())).isEqualTo(defaultQueue);
    }

    @Test
    public void otherwiseMethodShouldBeOptional() throws Exception {
        ContentBasedJmsQueueRecordWriter recordWriter = ContentBasedJmsQueueRecordWriterBuilder.newContentBasedJmsQueueRecordWriterBuilder()
                .when(predicate)
                .writeTo(queue)
                .build();

        assertThat(recordWriter).isNotNull();
        Map<Predicate, QueueSender> queueMap = recordWriter.getQueueMap();
        assertThat(queueMap).isNotNull();
        assertThat(queueMap.get(predicate)).isEqualTo(queue);
        assertThat(queueMap.get(new DefaultPredicate())).isNull();
    }

}
