/*
 * The MIT License
 *
 *  Copyright (c) 2020, Mahmoud Ben Hassine (mahmoud.benhassine@icloud.com)
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 *  THE SOFTWARE.
 */

package org.jeasy.batch.tutorials.intermediate.load;

import org.jeasy.batch.core.filter.HeaderRecordFilter;
import org.jeasy.batch.core.job.Job;
import org.jeasy.batch.core.job.JobExecutor;
import org.jeasy.batch.core.job.JobReport;
import org.jeasy.batch.flatfile.DelimitedRecordMapper;
import org.jeasy.batch.flatfile.FlatFileRecordReader;
import org.jeasy.batch.jdbc.BeanPropertiesPreparedStatementProvider;
import org.jeasy.batch.jdbc.JdbcRecordWriter;
import org.jeasy.batch.tutorials.common.DatabaseUtil;
import org.jeasy.batch.tutorials.common.Tweet;
import org.jeasy.batch.validation.BeanValidationRecordValidator;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.jeasy.batch.core.job.JobBuilder.aNewJob;

/**
 * Main class to run the JDBC import data tutorial.
 *
 * The goal is to read tweets from a CSV file and load them in a relational database.
 *
 * @author Mahmoud Ben Hassine (mahmoud.benhassine@icloud.com)
 */
public class Launcher {

    public static void main(String[] args) throws Exception {

        // Load tweets from tweets.csv
        Path tweets = Paths.get("src/main/resources/data/tweets.csv");

        // Start embedded database server
        DatabaseUtil.startEmbeddedDatabase();
        DatabaseUtil.deleteAllTweets();

        DataSource dataSource = DatabaseUtil.getDataSource();
        String query = "INSERT INTO tweet VALUES (?,?,?);";
        String[] fields = {"id", "user", "message"};

        // Build a batch job
        Job job = aNewJob()
                .batchSize(2)
                .reader(new FlatFileRecordReader(tweets))
                .filter(new HeaderRecordFilter())
                .mapper(new DelimitedRecordMapper<>(Tweet.class, fields))
                .validator(new BeanValidationRecordValidator())
                .writer(new JdbcRecordWriter(dataSource, query, new BeanPropertiesPreparedStatementProvider(Tweet.class, fields)))
                .build();
        
        // Execute the job
        JobExecutor jobExecutor = new JobExecutor();
        JobReport jobReport = jobExecutor.execute(job);
        jobExecutor.shutdown();

        System.out.println(jobReport);

        // Dump tweet table to check inserted data
        DatabaseUtil.dumpTweetTable();

        // Shutdown embedded database server and delete temporary files
        DatabaseUtil.cleanUpWorkingDirectory();

    }

}
