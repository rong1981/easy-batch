/**
 * The MIT License
 *
 *   Copyright (c) 2017, Mahmoud Ben Hassine (mahmoud.benhassine@icloud.com)
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
package org.easybatch.core.job;

import org.easybatch.core.filter.RecordFilter;
import org.easybatch.core.listener.*;
import org.easybatch.core.mapper.RecordMapper;
import org.easybatch.core.marshaller.RecordMarshaller;
import org.easybatch.core.processor.RecordProcessor;
import org.easybatch.core.reader.RecordReader;
import org.easybatch.core.skipper.RecordSkipper;
import org.easybatch.core.validator.RecordValidator;
import org.easybatch.core.writer.RecordWriter;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import static org.easybatch.core.util.Utils.checkNotNull;

/**
 * Batch job builder.
 * This is the main entry point to configure batch jobs.
 *
 * @author Mahmoud Ben Hassine (mahmoud.benhassine@icloud.com)
 */
public final class JobBuilder {

    private static final Logger LOGGER = Logger.getLogger(BatchJob.class.getName());

    private BatchJob job;

    private JobParameters parameters;

    static {
        try {
            if (System.getProperty("java.util.logging.config.file") == null &&
                    System.getProperty("java.util.logging.config.class") == null) {
                LogManager.getLogManager().readConfiguration(BatchJob.class.getResourceAsStream("/logging.properties"));
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Unable to load logging configuration file", e);
        }
    }

    /**
     * Create a new {@link JobBuilder}.
     */
    public JobBuilder() {
        parameters = new JobParameters();
        job = new BatchJob(parameters);
    }

    /**
     * Create a new {@link JobBuilder}.
     *
     * @return a new job builder.
     */
    public static JobBuilder aNewJob() {
        return new JobBuilder();
    }

    /**
     * Set the job name.
     *
     * @param name the job name
     * @return the job builder
     */
    public JobBuilder named(final String name) {
        checkNotNull(name, "job name");
        job.setName(name);
        return this;
    }

    /**
     * Register a record reader.
     *
     * @param recordReader the record reader to register
     * @return the job builder
     */
    public JobBuilder reader(final RecordReader recordReader) {
        checkNotNull(recordReader, "record reader");
        job.setRecordReader(recordReader);
        return this;
    }

    /**
     * Register a record reader.
     * This is an alias for {@link JobBuilder#reader(org.easybatch.core.reader.RecordReader)}
     *
     * @param recordReader the record reader to register
     * @return the job builder
     */
    public JobBuilder read(final RecordReader recordReader) {
        return reader(recordReader);
    }

    /**
     * Register a record filter.
     * @deprecated use {@link JobBuilder#skipper(RecordSkipper)} instead.
     *
     * @param recordFilter the record filter to register
     * @return the job builder
     */
    @Deprecated
    public JobBuilder filter(final RecordFilter recordFilter) {
        checkNotNull(recordFilter, "record filter");
        job.addRecordProcessor(recordFilter);
        return this;
    }

    /**
     * Register a record skipper.
     *
     * @param recordSkipper the record skipper to register
     * @return the job builder
     */
    public JobBuilder skipper(final RecordSkipper recordSkipper) {
        checkNotNull(recordSkipper, "record skipper");
        job.addRecordProcessor(recordSkipper);
        return this;
    }

    /**
     * Register a record skipper.
     * This is an alias for {@link JobBuilder#skipper(org.easybatch.core.skipper.RecordSkipper)}
     *
     * @param recordSkipper the record skipper to register
     * @return the job builder
     */
    public JobBuilder skip(final RecordSkipper recordSkipper) {
        return skipper(recordSkipper);
    }

    /**
     * Register a record mapper.
     *
     * @param recordMapper the record mapper to register
     * @return the job builder
     */
    public JobBuilder mapper(final RecordMapper recordMapper) {
        checkNotNull(recordMapper, "record mapper");
        job.addRecordProcessor(recordMapper);
        return this;
    }

    /**
     * Register a record mapper.
     * This is an alias for {@link JobBuilder#mapper(org.easybatch.core.mapper.RecordMapper)}
     *
     * @param recordMapper the record mapper to register
     * @return the job builder
     */
    public JobBuilder map(final RecordMapper recordMapper) {
        return mapper(recordMapper);
    }

    /**
     * Register a record validator.
     *
     * @param recordValidator the record validator to register
     * @return the job builder
     */
    public JobBuilder validator(final RecordValidator recordValidator) {
        checkNotNull(recordValidator, "record validator");
        job.addRecordProcessor(recordValidator);
        return this;
    }

    /**
     * Register a record validator.
     * This is an alias for {@link JobBuilder#validator(org.easybatch.core.validator.RecordValidator)}
     *
     * @param recordValidator the record validator to register
     * @return the job builder
     */
    public JobBuilder validate(final RecordValidator recordValidator) {
        return validator(recordValidator);
    }

    /**
     * Register a record processor.
     *
     * @param recordProcessor the record processor to register
     * @return the job builder
     */
    public JobBuilder processor(final RecordProcessor recordProcessor) {
        checkNotNull(recordProcessor, "record processor");
        job.addRecordProcessor(recordProcessor);
        return this;
    }

    /**
     * Register a record processor.
     * This is an alias for {@link JobBuilder#processor(org.easybatch.core.processor.RecordProcessor)}
     *
     * @param recordProcessor the record processor to register
     * @return the job builder
     */
    public JobBuilder process(final RecordProcessor recordProcessor) {
        return processor(recordProcessor);
    }

    /**
     * Register a record marshaller.
     *
     * @param recordMarshaller the record marshaller to register
     * @return the job builder
     */
    public JobBuilder marshaller(final RecordMarshaller recordMarshaller) {
        checkNotNull(recordMarshaller, "record marshaller");
        job.addRecordProcessor(recordMarshaller);
        return this;
    }

    /**
     * Register a record marshaller.
     * This is an alias to {@link JobBuilder#marshaller(org.easybatch.core.marshaller.RecordMarshaller)}
     *
     * @param recordMarshaller the record marshaller to register
     * @return the job builder
     */
    public JobBuilder marshal(final RecordMarshaller recordMarshaller) {
        return marshaller(recordMarshaller);
    }

    /**
     * Register a record writer.
     *
     * @param recordWriter the record writer to register
     * @return the job builder
     */
    public JobBuilder writer(final RecordWriter recordWriter) {
        checkNotNull(recordWriter, "record writer");
        job.setRecordWriter(recordWriter);
        return this;
    }

    /**
     * Register a record writer.
     * This is an alias for {@link JobBuilder#writer(org.easybatch.core.writer.RecordWriter)}
     *
     * @param recordWriter the record writer to register
     * @return the job builder
     */
    public JobBuilder write(final RecordWriter recordWriter) {
        return writer(recordWriter);
    }

    /**
     * Set a threshold for errors. The job will be aborted if the threshold is exceeded.
     *
     * @param errorThreshold the error threshold
     * @return the job builder
     */
    public JobBuilder errorThreshold(final long errorThreshold) {
        if (errorThreshold < 1) {
            throw new IllegalArgumentException("error threshold must be >= 1");
        }
        parameters.setErrorThreshold(errorThreshold);
        return this;
    }

    /**
     * Activate JMX monitoring.
     *
     * @param jmx true to enable jmx monitoring
     * @return the job builder
     */
    public JobBuilder enableJmx(final boolean jmx) {
        parameters.setJmxMonitoring(jmx);
        return this;
    }

    /**
     * Set the batch size.
     *
     * @param batchSize the batch size
     * @return the job builder
     */
    public JobBuilder batchSize(final int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("Batch size must be >= 1");
        }
        parameters.setBatchSize(batchSize);
        return this;
    }

    /**
     * Register a job listener.
     * See {@link JobListener} for available callback methods.
     *
     * @param jobListener The job listener to add.
     * @return the job builder
     */
    public JobBuilder jobListener(final JobListener jobListener) {
        checkNotNull(jobListener, "job listener");
        job.addJobListener(jobListener);
        return this;
    }

    /**
     * Register a job listener.
     * See {@link JobListener} for available callback methods.
     *
     * @param jobListener The job listener to add.
     * @return the job builder
     */
    public JobBuilder apply(final JobListener jobListener) {
        return jobListener(jobListener);
    }

    /**
     * Register a batch listener.
     * See {@link BatchListener} for available callback methods.
     *
     * @param batchListener The batch listener to add.
     * @return the job builder
     */
    public JobBuilder batchListener(final BatchListener batchListener) {
        checkNotNull(batchListener, "batch listener");
        job.addBatchListener(batchListener);
        return this;
    }

    public JobBuilder apply(final BatchListener batchListener) {
        return batchListener(batchListener);
    }

    /**
     * Register a record reader listener.
     * See {@link RecordReaderListener} for available callback methods.
     *
     * @param recordReaderListener The record reader listener to add.
     * @return the job builder
     */
    public JobBuilder readerListener(final RecordReaderListener recordReaderListener) {
        checkNotNull(recordReaderListener, "record reader listener");
        job.addRecordReaderListener(recordReaderListener);
        return this;
    }

    public JobBuilder apply(final RecordReaderListener recordReaderListener) {
        return readerListener(recordReaderListener);
    }

    /**
     * Register a pipeline listener.
     * See {@link PipelineListener} for available callback methods.
     *
     * @param pipelineListener The pipeline listener to add.
     * @return the job builder
     */
    public JobBuilder pipelineListener(final PipelineListener pipelineListener) {
        checkNotNull(pipelineListener, "pipeline listener");
        job.addPipelineListener(pipelineListener);
        return this;
    }

    public JobBuilder apply(final PipelineListener pipelineListener) {
        return pipelineListener(pipelineListener);
    }

    /**
     * Register a record writer listener.
     * See {@link RecordWriterListener} for available callback methods.
     *
     * @param recordWriterListener The record writer listener to register.
     * @return the job builder
     */
    public JobBuilder writerListener(final RecordWriterListener recordWriterListener) {
        checkNotNull(recordWriterListener, "record writer listener");
        job.addRecordWriterListener(recordWriterListener);
        return this;
    }

    public JobBuilder apply(final RecordWriterListener recordWriterListener) {
        return writerListener(recordWriterListener);
    }

    /**
     * Build a batch job instance.
     *
     * @return a batch job instance
     */
    public Job build() {
        return job;
    }

}
