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
package org.jeasy.batch.core.jmx;

import org.jeasy.batch.core.job.JobReport;

import javax.management.AttributeChangeNotification;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.remote.JMXConnectionNotification;

/**
 * Listener for job report updates sent via JMX by the job monitor.
 *
 * @author Mahmoud Ben Hassine (mahmoud.benhassine@icloud.com)
 */
public abstract class JobMonitoringListener implements NotificationListener {

    /**
     * Method invoked when a new job report update is received.
     *
     * @param jobReport the new job report update
     */
    public abstract void onJobReportUpdate(final JobReport jobReport);

    /**
     * Method invoked when the JMX connection is opened.
     */
    public abstract void onConnectionOpened();

    /**
     * Method invoked when the JMX connection is closed.
     */
    public abstract void onConnectionClosed();

    /**
     * {@inheritDoc}
     */
    public void handleNotification(Notification notification, Object handback) {
        if (notification instanceof AttributeChangeNotification) {
            AttributeChangeNotification attributeChangeNotification = (AttributeChangeNotification) notification;
            JobReport jobReport = (JobReport)attributeChangeNotification.getNewValue();
            onJobReportUpdate(jobReport);
        }

        if (notification instanceof JMXConnectionNotification) {
            JMXConnectionNotification jmxConnectionNotification = (JMXConnectionNotification) notification;
            String type = jmxConnectionNotification.getType();
            switch (type) {
                case JMXConnectionNotification.OPENED: onConnectionOpened(); break;
                case JMXConnectionNotification.CLOSED: onConnectionClosed(); break;
                default: break;
            }
        }

    }

}
