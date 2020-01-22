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
package org.jeasy.batch.core.listener;

import org.jeasy.batch.core.job.JobReport;
import org.jeasy.batch.core.job.JobParameters;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * Composite listener that delegates processing to other listeners.
 *
 * @author Mahmoud Ben Hassine (mahmoud.benhassine@icloud.com)
 */
public class CompositeJobListener implements JobListener {

    private List<JobListener> listeners;

    /**
     * Create a new {@link CompositeJobListener}.
     */
    public CompositeJobListener() {
        this(new ArrayList<JobListener>());
    }

    /**
     * Create a new {@link CompositeJobListener}.
     *
     * @param listeners delegates
     */
    public CompositeJobListener(List<JobListener> listeners) {
        this.listeners = listeners;
    }

    @Override
    public void beforeJobStart(JobParameters jobParameters) {
        for (JobListener listener : listeners) {
            listener.beforeJobStart(jobParameters);
        }
    }

    @Override
    public void afterJobEnd(JobReport jobReport) {
        for (ListIterator<JobListener> iterator
                = listeners.listIterator(listeners.size());
                iterator.hasPrevious();) {
            iterator.previous().afterJobEnd(jobReport);
        }
    }

    /**
     * Add a delegate listener.
     *
     * @param jobListener to add
     */
    public void addJobListener(JobListener jobListener) {
        listeners.add(jobListener);
    }
}
