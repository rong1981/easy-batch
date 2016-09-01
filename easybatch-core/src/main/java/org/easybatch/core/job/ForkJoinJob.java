package org.easybatch.core.job;


import org.easybatch.core.record.Record;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ForkJoinJob implements Job {

    ExecutorService executorService;

    private int degreeOfParallelism;

    private List<BlockingQueue<Record>> workingQueues;

    private BlockingQueue<Record> joinQueue;

    private ForkJob forkJob;
    private JoinJob joinJob;
    private List<Job> workerJobs;

    public ForkJoinJob() {
        executorService = Executors.newFixedThreadPool(degreeOfParallelism);
    }

    public ForkJoinJob(ForkJob forkJob, List<Job> workerJobs, JoinJob joinJob, int degreeOfParallelism) {
        this.degreeOfParallelism = degreeOfParallelism;
        this.forkJob = forkJob;
        this.joinJob = joinJob;
        this.workerJobs = workerJobs;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public String getExecutionId() {
        return null;
    }

    @Override
    public JobReport call() {
        JobReportMerger reportMerger;
        List<Job> jobs = new ArrayList<>();
        jobs.add(forkJob);
        jobs.addAll(workerJobs);
        jobs.add(joinJob);
        try {
            List<Future<JobReport>> futures = executorService.invokeAll(jobs);
            // merge reports
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        executorService.shutdown();
        return null;
    }

}
