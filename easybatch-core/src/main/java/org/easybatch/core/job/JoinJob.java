package org.easybatch.core.job;

import org.easybatch.core.filter.PoisonRecordFilter;
import org.easybatch.core.reader.BlockingQueueRecordReader;
import org.easybatch.core.record.Record;
import org.easybatch.core.writer.StandardOutputRecordWriter;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;

import static org.easybatch.core.job.JobBuilder.aNewJob;

public class JoinJob implements Job {

    private int degreeOfParallelism;

    private BlockingQueue<Record> joinQueue;

    public JoinJob(BlockingQueue<Record> joinQueue, int degreeOfParallelism) {
        this.degreeOfParallelism = degreeOfParallelism;
        this.joinQueue = joinQueue;
    }

    @Override
    public String getName() {
        return "join-job";
    }

    @Override
    public String getExecutionId() {
        return UUID.randomUUID().toString();
    }

    @Override
    public JobReport call() {
        return aNewJob()
                .named("join-job")
                .reader(new BlockingQueueRecordReader<>(joinQueue, degreeOfParallelism))
                .filter(new PoisonRecordFilter())
                .writer(new StandardOutputRecordWriter())
                .build().call();
    }
}
