package org.easybatch.core;

import org.easybatch.core.dispatcher.PoisonRecordBroadcaster;
import org.easybatch.core.dispatcher.RoundRobinRecordDispatcher;
import org.easybatch.core.filter.PoisonRecordFilter;
import org.easybatch.core.job.Job;
import org.easybatch.core.processor.RecordProcessingException;
import org.easybatch.core.processor.RecordProcessor;
import org.easybatch.core.reader.BlockingQueueRecordReader;
import org.easybatch.core.reader.IterableRecordReader;
import org.easybatch.core.record.Record;
import org.easybatch.core.writer.BlockingQueueRecordWriter;
import org.easybatch.core.writer.StandardOutputRecordWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.Arrays.asList;
import static org.easybatch.core.job.JobBuilder.aNewJob;

public class DispatchAggregateSample {

    private static final int THREAD_POOL_SIZE = 4;

    public static void main(String[] args) throws Exception {

        /*Job job = new ForkJoinJobBuilder(10)
                .fork()
                .join()
                .build();*/

        List<String> dataSource = Arrays.asList("foo", "bar", "toto", "titi");

        // Create queues
        BlockingQueue<Record> queue1 = new LinkedBlockingQueue<>();
        BlockingQueue<Record> queue2 = new LinkedBlockingQueue<>();
        BlockingQueue<Record> aggregationQueue = new LinkedBlockingQueue<>();

        // Create a round robin record dispatcher to distribute records to worker jobs
        RoundRobinRecordDispatcher<Record> roundRobinRecordDispatcher =
                new RoundRobinRecordDispatcher<>(asList(queue1, queue2));

        // Build a master job to read records from the data source and dispatch them to worker jobs
        Job masterJob = aNewJob()
                .named("master-job")
                .reader(new IterableRecordReader(dataSource))
                .dispatcher(roundRobinRecordDispatcher)
                .jobListener(new PoisonRecordBroadcaster<>(asList(queue1, queue2)))
                .build();

        // Build worker jobs, will write records to aggregationQueue
        Job workerJob1 = buildWorkerJob(queue1, "worker-job1", aggregationQueue);
        Job workerJob2 = buildWorkerJob(queue2, "worker-job2", aggregationQueue);
        // Read from the aggregating queue
        Job workerJob3 = buildAggregatingJob(aggregationQueue, "aggregating-job");

        // Create a thread pool to call master and worker jobs in parallel
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        // Submit workers to executor service
        executorService.invokeAll(asList(masterJob, workerJob1, workerJob2, workerJob3));

        // Shutdown executor service
        executorService.shutdown();

    }

    public static Job buildWorkerJob(BlockingQueue<Record> workQueue, String jobName, BlockingQueue<Record> aggregationQueue) {
        return aNewJob()
                .named(jobName)
                .reader(new BlockingQueueRecordReader<>(workQueue))
                .processor(new DummyProcessor())
                .writer(new BlockingQueueRecordWriter<>(aggregationQueue))
                .build();
    }

    public static Job buildAggregatingJob(BlockingQueue<Record> queue, String jobName) throws IOException {
        return aNewJob()
                .named(jobName)
                .reader(new BlockingQueueRecordReader<>(queue, 2)) // stop reading after receiving 2 poison records (one from each worker job)
                .filter(new PoisonRecordFilter())
                .writer(new StandardOutputRecordWriter())
                .build();
    }

    static class DummyProcessor implements RecordProcessor<Record, Record> {
        @Override
        public Record processRecord(Record record) throws RecordProcessingException {
            // can ignore poison records here, but don't filter them with a record filter in the pipeline
            System.out.println("processing record: " + record.getPayload());
            return record;
        }
    }

}