package org.easybatch.core;

import org.easybatch.core.record.Record;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Created by benas on 07/07/16.
 */
public class ForkJoinJobBuilder {

    private List<BlockingQueue<Record>> workingQueues;

    private BlockingQueue<Record> jooinQueue;

    public ForkJoinJobBuilder(int degreeOfParallelism) {
    }
}
