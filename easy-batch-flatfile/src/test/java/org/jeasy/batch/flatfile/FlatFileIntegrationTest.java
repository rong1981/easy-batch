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
package org.jeasy.batch.flatfile;

import org.jeasy.batch.core.converter.DateTypeConverter;
import org.jeasy.batch.core.converter.TypeConverter;
import org.jeasy.batch.core.filter.HeaderRecordFilter;
import org.jeasy.batch.core.job.Job;
import org.jeasy.batch.core.job.JobBuilder;
import org.jeasy.batch.core.job.JobExecutor;
import org.jeasy.batch.core.job.JobReport;
import org.jeasy.batch.core.job.JobStatus;
import org.jeasy.batch.core.processor.RecordCollector;
import org.jeasy.batch.core.record.Record;
import org.jeasy.batch.core.util.Utils;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class FlatFileIntegrationTest {

    @Test
    public void testCsvProcessing() throws Exception {

        Path dataSource = Paths.get("src/test/resources/persons.csv");

        RecordCollector<Person> recordCollector = new RecordCollector<>();
        Job job = JobBuilder.aNewJob()
                .reader(new FlatFileRecordReader(dataSource))
                .mapper(new DelimitedRecordMapper<>(Person.class, "firstName", "lastName", "age", "birthDate", "married"))
                .processor(recordCollector)
                .build();

        JobReport jobReport = new JobExecutor().execute(job);

        assertThat(jobReport).isNotNull();
        assertThat(jobReport.getMetrics().getErrorCount()).isEqualTo(0);
        assertThat(jobReport.getMetrics().getFilterCount()).isEqualTo(0);
        assertThat(jobReport.getMetrics().getWriteCount()).isEqualTo(2);
        assertThat(jobReport.getStatus()).isEqualTo(JobStatus.COMPLETED);
        assertThat(jobReport.getMetrics().getReadCount()).isEqualTo(2);

        List<Record<Person>> records = recordCollector.getRecords();
        assertPersons(Utils.extractPayloads(records));

    }

    @Test
    public void testCsvSubRecordProcessing() throws Exception {

        Path dataSource = Paths.get("src/test/resources/persons.csv");

        RecordCollector<Person> recordCollector = new RecordCollector<>();
        Job job = JobBuilder.aNewJob()
                .reader(new FlatFileRecordReader(dataSource))
                .mapper(new DelimitedRecordMapper<>(Person.class, new Integer[]{2, 4}, new String[]{"age", "married"}))
                .processor(recordCollector)
                .build();

        JobReport jobReport = new JobExecutor().execute(job);

        assertThat(jobReport).isNotNull();
        assertThat(jobReport.getMetrics().getErrorCount()).isEqualTo(0);
        assertThat(jobReport.getMetrics().getFilterCount()).isEqualTo(0);
        assertThat(jobReport.getMetrics().getWriteCount()).isEqualTo(2);
        assertThat(jobReport.getStatus()).isEqualTo(JobStatus.COMPLETED);
        assertThat(jobReport.getMetrics().getReadCount()).isEqualTo(2);

        List<Record<Person>> records = recordCollector.getRecords();

        assertPersonsFieldSubsetMapping(Utils.extractPayloads(records));

    }

    /*
     * Test field names convention over configuration
     */

    @Test
    public void whenFieldNamesAreNotSpecified_thenTheyShouldBeRetrievedFromTheHeaderRecord() throws Exception {

        Path dataSource = Paths.get("src/test/resources/persons_with_header.csv");

        RecordCollector<Person> recordCollector = new RecordCollector<>();
        Job job = JobBuilder.aNewJob()
                .reader(new FlatFileRecordReader(dataSource))
                .mapper(new DelimitedRecordMapper<>(Person.class))
                .processor(recordCollector)
                .build();

        JobReport jobReport = new JobExecutor().execute(job);

        assertThat(jobReport).isNotNull();
        assertThat(jobReport.getMetrics().getErrorCount()).isEqualTo(1);
        assertThat(jobReport.getMetrics().getFilterCount()).isEqualTo(0);
        assertThat(jobReport.getMetrics().getWriteCount()).isEqualTo(2);
        assertThat(jobReport.getStatus()).isEqualTo(JobStatus.COMPLETED);
        assertThat(jobReport.getMetrics().getReadCount()).isEqualTo(3);

        List<Record<Person>> records = recordCollector.getRecords();
        assertPersons(Utils.extractPayloads(records));

    }

    @Test
    public void whenOnlySubsetOfFieldsAreSpecified_thenOnlyCorrespondingFieldsShouldBeMapped() throws Exception {

        Path dataSource = Paths.get("src/test/resources/persons_with_header.csv");

        RecordCollector<Person> recordCollector = new RecordCollector<>();
        Job job = JobBuilder.aNewJob()
                .reader(new FlatFileRecordReader(dataSource))
                .mapper(new DelimitedRecordMapper<>(Person.class, 2, 4))
                .processor(recordCollector)
                .build();

        JobReport jobReport = new JobExecutor().execute(job);

        assertThat(jobReport).isNotNull();
        assertThat(jobReport.getMetrics().getErrorCount()).isEqualTo(1);
        assertThat(jobReport.getMetrics().getFilterCount()).isEqualTo(0);
        assertThat(jobReport.getMetrics().getWriteCount()).isEqualTo(2);
        assertThat(jobReport.getStatus()).isEqualTo(JobStatus.COMPLETED);
        assertThat(jobReport.getMetrics().getReadCount()).isEqualTo(3);

        List<Record<Person>> records = recordCollector.getRecords();
        assertPersonsFieldSubsetMapping(Utils.extractPayloads(records));

    }

    @Test
    public void testComplaintsDataProcessing() throws Exception {

        //source: http://catalog.data.gov/dataset/consumer-complaint-database
        Path dataSource = Paths.get("src/test/resources/complaints.csv");

        DelimitedRecordMapper<Complaint> recordMapper = new DelimitedRecordMapper<>(Complaint.class,
                "id", "product", "subProduct", "issue", "subIssue", "state", "zipCode", "channel",
                "receivedDate", "sentDate", "company", "companyResponse", "timelyResponse", "consumerDisputed");
        recordMapper.registerTypeConverter(new TypeConverter<String, Channel>() {
            @Override
            public Channel convert(String value) {
                return Channel.valueOf(value.toUpperCase());
            }
        });
        recordMapper.registerTypeConverter(new DateTypeConverter("MM/dd/yyyy"));

        RecordCollector<Complaint> recordCollector = new RecordCollector<>();
        Job job = JobBuilder.aNewJob()
                .reader(new FlatFileRecordReader(dataSource))
                .filter(new HeaderRecordFilter())
                .mapper(recordMapper)
                .processor(recordCollector)
                .build();

        JobReport jobReport = new JobExecutor().execute(job);

        assertThat(jobReport).isNotNull();

        List<Record<Complaint>> records = recordCollector.getRecords();
        List<Complaint> complaints = Utils.extractPayloads(records);

        assertThat(complaints).isNotEmpty().hasSize(10);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MM/dd/yyyy");
        assertComplaint(complaints.get(0),
                1355160, "Student loan", "Non-federal student loan",
                "Dealing with my lender or servicer", null, "NJ",
                "08807", Channel.WEB,
                simpleDateFormat.parse("04/30/2015"), simpleDateFormat.parse("04/30/2015"),
                "Transworld Systems Inc.", "In progress", true, false);

        assertComplaint(complaints.get(9),
                1351334, "Money transfers", "International money transfer",
                "Money was not available when promised", null, "TX", "78666", Channel.PHONE,
                simpleDateFormat.parse("04/28/2015"), simpleDateFormat.parse("04/29/2015"),
                "MoneyGram", "In progress", true, false);

    }

    /*
     * Test fixed-length record processing
     */

    @Test
    public void testFlrProcessing() throws Exception {

        Path dataSource = Paths.get("src/test/resources/persons.flr");

        RecordCollector<Person> recordCollector = new RecordCollector<>();
        Job job = JobBuilder.aNewJob()
                .reader(new FlatFileRecordReader(dataSource))
                .mapper(new FixedLengthRecordMapper<>(Person.class, new int[]{4, 4, 2, 10, 1},
                        new String[]{"firstName", "lastName", "age", "birthDate", "married"}))
                .processor(recordCollector)
                .build();

        JobReport jobReport = new JobExecutor().execute(job);

        assertThat(jobReport).isNotNull();
        assertThat(jobReport.getMetrics().getErrorCount()).isEqualTo(0);
        assertThat(jobReport.getMetrics().getFilterCount()).isEqualTo(0);
        assertThat(jobReport.getMetrics().getWriteCount()).isEqualTo(2);
        assertThat(jobReport.getStatus()).isEqualTo(JobStatus.COMPLETED);
        assertThat(jobReport.getMetrics().getReadCount()).isEqualTo(2);

        List<Record<Person>> records = recordCollector.getRecords();
        List<Person> persons = Utils.extractPayloads(records);

        assertThat(persons).hasSize(2);

        assertPerson(persons.get(0), "foo ", "bar ", 30, true);
        assertPerson(persons.get(1), "toto", "titi", 15, false);

    }

    private void assertPersonsFieldSubsetMapping(List<Person> persons) {
        assertThat(persons).isNotEmpty().hasSize(2);

        Person person = persons.get(0);
        assertThat(person.getFirstName()).isNull();
        assertThat(person.getLastName()).isNull();
        assertThat(person.getBirthDate()).isNull();
        assertThat(person.getAge()).isEqualTo(30);
        assertThat(person.isMarried()).isTrue();

        person = persons.get(1);
        assertThat(person.getFirstName()).isNull();
        assertThat(person.getLastName()).isNull();
        assertThat(person.getBirthDate()).isNull();
        assertThat(person.getAge()).isEqualTo(15);
        assertThat(person.isMarried()).isFalse();
    }

    private void assertPersons(List<Person> persons) {
        assertThat(persons).isNotEmpty().hasSize(2);
        assertPerson(persons.get(0), "foo", "bar", 30, true);
        assertPerson(persons.get(1), "bar", "foo", 15, false);
    }

    private void assertPerson(Person person, String firstName, String lastName, int age, boolean married) {
        assertThat(person.getFirstName()).isEqualTo(firstName);
        assertThat(person.getLastName()).isEqualTo(lastName);
        assertThat(person.getAge()).isEqualTo(age);
        assertThat(person.isMarried()).isEqualTo(married);
    }

    private void assertComplaint(Complaint complaint, int id, String product, String subProduct, String issue, String subIssue,
                                 String state, String zipCode, Channel channel, Date receivedDate, Date sentDate,
                                 String company, String companyResponse, boolean timelyResponse, boolean consumerDisputed) {

        assertThat(complaint).isNotNull();
        assertThat(complaint.getId()).isEqualTo(id);
        assertThat(complaint.getProduct()).isEqualTo(product);
        assertThat(complaint.getSubProduct()).isEqualTo(subProduct);
        assertThat(complaint.getIssue()).isEqualTo(issue);
        assertThat(complaint.getSubIssue()).isEqualTo(subIssue);
        assertThat(complaint.getState()).isEqualTo(state);
        assertThat(complaint.getZipCode()).isEqualTo(zipCode);
        assertThat(complaint.getChannel()).isEqualTo(channel);
        assertThat(complaint.getReceivedDate()).isEqualTo(receivedDate);
        assertThat(complaint.getSentDate()).isEqualTo(sentDate);
        assertThat(complaint.getCompany()).isEqualTo(company);
        assertThat(complaint.getCompanyResponse()).isEqualTo(companyResponse);
        assertThat(complaint.isTimelyResponse()).isEqualTo(timelyResponse);
        assertThat(complaint.isConsumerDisputed()).isEqualTo(consumerDisputed);
    }

}
