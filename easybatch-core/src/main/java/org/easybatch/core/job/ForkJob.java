package org.easybatch.core.job;

import org.easybatch.core.dispatcher.PoisonRecordBroadcaster;
import org.easybatch.core.dispatcher.RoundRobinRecordDispatcher;
import org.easybatch.core.reader.RecordReader;
import org.easybatch.core.record.Record;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

import static org.easybatch.core.job.JobBuilder.aNewJob;

public class ForkJob implements Job {

    private RecordReader recordReader;

    private List<BlockingQueue<Record>> workingQueues;

    public ForkJob(RecordReader recordReader, List<BlockingQueue<Record>> workingQueues) {
        this.recordReader = recordReader;
        this.workingQueues = workingQueues;
    }

    @Override
    public String getName() {
        return "fork-job";
    }

    @Override
    public String getExecutionId() {
        return UUID.randomUUID().toString();
    }

    @Override
    public JobReport call() {
        return aNewJob()
                .named("fork-job")
                .reader(recordReader)
                .dispatcher(new RoundRobinRecordDispatcher<>(workingQueues))
                .jobListener(new PoisonRecordBroadcaster<>(workingQueues))
                .build().call();
    }
}
