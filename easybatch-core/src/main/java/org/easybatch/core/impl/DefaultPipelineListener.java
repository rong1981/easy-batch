/*
 *  The MIT License
 *
 *   Copyright (c) 2015, Mahmoud Ben Hassine (mahmoud@benhassine.fr)
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

package org.easybatch.core.impl;

import org.easybatch.core.api.JobReport;
import org.easybatch.core.api.listener.PipelineListener;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Listener that logs and reports filtered/error records.
 *
 * @author Mahmoud Ben Hassine (mahmoud@benhassine.fr)
 */
class DefaultPipelineListener implements PipelineListener {

    private static final Logger LOGGER = Logger.getLogger(DefaultPipelineListener.class.getName());

    private JobReport jobReport;

    DefaultPipelineListener(JobReport jobReport) {
        this.jobReport = jobReport;
    }

    /**
     * Called before the record gets processed.
     * If you create a new record, you <strong>must</strong> keep the original header of the modified record.
     *
     * @param record The record that will be processed.
     */
    @Override
    public Object beforeRecordProcessing(Object record) {
        return record;
    }

    /**
     * Called after the record has been processed.
     *
     * @param inputRecord  The record to process.
     * @param outputRecord The processed record. <strong>May be null if the record has been filtered</strong>
     */
    @Override
    public void afterRecordProcessing(Object inputRecord, Object outputRecord) {
        if (outputRecord == null) {
            LOGGER.log(Level.INFO, "Record {0} has been filtered", inputRecord);
            jobReport.getMetrics().incrementFilteredCount();
        } else {
            jobReport.getMetrics().incrementSuccessCount();
        }
    }

    /**
     * Called when an exception occurs during record processing
     *
     * @param record    the record attempted to be processed
     * @param throwable the exception occurred during record processing
     */
    @Override
    public void onRecordProcessingException(Object record, Throwable throwable) {
        LOGGER.log(Level.SEVERE, "An exception occurred while processing record " + record, throwable);
        jobReport.getMetrics().incrementErrorCount();
    }
}
