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
package org.jeasy.batch.jms;

import org.jeasy.batch.core.record.Batch;
import org.jeasy.batch.core.record.Record;
import org.jeasy.batch.core.writer.RecordWriter;

import javax.jms.*;

import static org.jeasy.batch.core.util.Utils.checkNotNull;

/**
 * Sends a Jms message to a given queue.
 *
 * @author Mahmoud Ben Hassine (mahmoud.benhassine@icloud.com)
 */
public class JmsQueueRecordWriter implements RecordWriter {

    private QueueConnectionFactory queueConnectionFactory;
    private QueueConnection queueConnection;
    private QueueSession queueSession;
    private QueueSender queueSender;
    private Queue queue;

    /**
     * Create a new {@link JmsQueueRecordWriter}.
     *
     * @param queueConnectionFactory to use to create connections.
     * @param queue                  the target queue to write records to
     * @throws JMSException if an exception occurs while sending the Jms message
     */
    public JmsQueueRecordWriter(final QueueConnectionFactory queueConnectionFactory, final Queue queue) throws JMSException {
        checkNotNull(queueConnectionFactory, "queue connection factory");
        checkNotNull(queue, "queue");
        this.queueConnectionFactory = queueConnectionFactory;
        this.queue = queue;
    }

    @Override
    public void open() throws Exception {
        queueConnection = queueConnectionFactory.createQueueConnection();
        queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        queueSender = queueSession.createSender(queue);
    }

    @Override
    public void writeRecords(Batch batch) throws Exception {
        for (Record record : batch) {
            queueSender.send((Message) record.getPayload());
        }
    }

    @Override
    public void close() throws Exception {
        if (queueSender != null) {
            queueSender.close();
        }
        if (queueSession != null) {
            queueSession.close();
        }
        if (queueConnection != null) {
            queueConnection.close();
        }
    }
}
