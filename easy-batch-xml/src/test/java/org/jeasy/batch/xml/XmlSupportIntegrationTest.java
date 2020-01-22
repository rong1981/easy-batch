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
package org.jeasy.batch.xml;

import org.assertj.core.api.Assertions;
import org.jeasy.batch.core.job.Job;
import org.jeasy.batch.core.job.JobBuilder;
import org.jeasy.batch.core.job.JobExecutor;
import org.jeasy.batch.core.job.JobReport;
import org.jeasy.batch.core.job.JobStatus;
import org.jeasy.batch.core.processor.RecordCollector;
import org.jeasy.batch.core.record.Record;
import org.jeasy.batch.core.util.Utils;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class XmlSupportIntegrationTest {

    @Test
    public void testWebsitesProcessing() throws Exception {
        // given
        final InputStream xmlDataSource = getDataSource("/websites.xml");
        RecordCollector<Website> recordCollector = new RecordCollector<>();
        Job job = JobBuilder.aNewJob()
                .reader(new XmlRecordReader(xmlDataSource, "website"))
                .mapper(new XmlRecordMapper<>(Website.class))
                .processor(recordCollector)
                .build();

        // when
        JobExecutor jobExecutor = new JobExecutor();
        jobExecutor.execute(job);
        jobExecutor.shutdown();

        // then
        List<Record<Website>> records = recordCollector.getRecords();
        List<Website> websites = Utils.extractPayloads(records);

        assertThat(websites).hasSize(5);

        Website website = websites.get(0);
        assertThat(website.getName()).isEqualTo("google");
        assertThat(website.getUrl()).isEqualTo("http://www.google.com?query=test&sort=asc");

        website = websites.get(1);
        assertThat(website.getName()).isEqualTo("l'equipe");
        assertThat(website.getUrl()).isEqualTo("http://www.lequipe.fr");

        website = websites.get(2);
        assertThat(website.getName()).isEqualTo("l\"internaute.com");
        assertThat(website.getUrl()).isEqualTo("http://www.linternaute.com");
    }

    @Test
    public void testPersonsProcessing() throws Exception {
        // given
        final InputStream xmlDataSource = getDataSource("/persons.xml");

        RecordCollector<Person> recordCollector = new RecordCollector<>();
        Job job = JobBuilder.aNewJob()
                .reader(new XmlRecordReader(xmlDataSource, "person"))
                .mapper(new XmlRecordMapper<>(Person.class))
                .processor(recordCollector)
                .build();

        // when
        JobExecutor jobExecutor = new JobExecutor();
        JobReport jobReport = jobExecutor.execute(job);
        jobExecutor.shutdown();

        // then
        assertThatReportIsCorrect(jobReport);

        List<Record<Person>> records = recordCollector.getRecords();
        List<Person> persons = Utils.extractPayloads(records);

        assertThat(persons).hasSize(2);

        Person person = persons.get(0);
        assertThat(person.getId()).isEqualTo(1);
        assertThat(person.getFirstName()).isEqualTo("foo");
        assertThat(person.getLastName()).isEqualTo("bar");
        assertThat(person.isMarried()).isTrue();

        person = persons.get(1);
        assertThat(person.getId()).isEqualTo(2);
        assertThat(person.getFirstName()).isEqualTo("bar");
        assertThat(person.getLastName()).isEqualTo("foo");
        assertThat(person.isMarried()).isFalse();
    }

    @Test
    public void testMavenDependenciesProcessing() throws Exception {
        // given
        final InputStream xmlDataSource = getDataSource("/dependencies.xml");

        RecordCollector<Dependency> recordCollector = new RecordCollector<>();
        Job job = JobBuilder.aNewJob()
                .reader(new XmlRecordReader(xmlDataSource, "dependency"))
                .mapper(new XmlRecordMapper<>(Dependency.class))
                .processor(recordCollector)
                .build();

        // when
        JobExecutor jobExecutor = new JobExecutor();
        JobReport jobReport = jobExecutor.execute(job);
        jobExecutor.shutdown();

        // then
        assertThatReportIsCorrect(jobReport);

        List<Record<Dependency>> records = recordCollector.getRecords();
        List<Dependency> dependencies = Utils.extractPayloads(records);

        assertThat(dependencies).hasSize(2);

        Dependency dependency = dependencies.get(0);
        assertThat(dependency).isNotNull();
        assertThat(dependency.getArtifactId()).isEqualTo("junit");
        assertThat(dependency.getGroupId()).isEqualTo("junit");
        assertThat(dependency.getVersion()).isEqualTo("4.12");
        assertThat(dependency.getScope()).isEqualTo("test");
        assertThat(dependency.getClassifier()).isNull();
        assertThat(dependency.getSystemPath()).isNull();
        assertThat(dependency.getType()).isNull();
        assertThat(dependency.getExclusions()).isNull();
        assertThat(dependency.isOptional()).isFalse();

        dependency = dependencies.get(1);
        assertThat(dependency).isNotNull();
        assertThat(dependency.getArtifactId()).isEqualTo("fake-core");
        assertThat(dependency.getGroupId()).isEqualTo("org.fake");
        assertThat(dependency.getVersion()).isEqualTo("1.0");
        assertThat(dependency.getScope()).isNull();
        assertThat(dependency.getClassifier()).isNull();
        assertThat(dependency.getSystemPath()).isNull();
        assertThat(dependency.getType()).isNull();
        assertThat(dependency.isOptional()).isTrue();

        Dependency.Exclusions exclusions = dependency.getExclusions();
        assertThat(exclusions).isNotNull();
        Assertions.assertThat(exclusions.getExclusion()).hasSize(1);

        Exclusion exclusion = exclusions.getExclusion().get(0);
        assertThat(exclusion).isNotNull();
        assertThat(exclusion.getGroupId()).isEqualTo("some.excluded.dep");
        assertThat(exclusion.getArtifactId()).isEqualTo("dep-core");
    }

    @Test
    public void testXmlProcessingWithCustomNamespace() throws Exception {
        // given
        final InputStream xmlDataSource = getDataSource("/beans.xml");

        RecordCollector<Bean> recordCollector = new RecordCollector<>();
        Job job = JobBuilder.aNewJob()
                .reader(new XmlRecordReader(xmlDataSource, "bean"))
                .mapper(new XmlRecordMapper<>(Bean.class))
                .processor(recordCollector)
                .build();

        // when
        JobExecutor jobExecutor = new JobExecutor();
        JobReport jobReport = jobExecutor.execute(job);
        jobExecutor.shutdown();

        // then
        assertThat(jobReport).isNotNull();
        assertThat(jobReport.getMetrics().getReadCount()).isEqualTo(2);

        List<Record<Bean>> records = recordCollector.getRecords();
        List<Bean> beans = Utils.extractPayloads(records);

        assertThat(beans).isNotEmpty().hasSize(2);

        Bean bean = beans.get(0);
        assertThat(bean).isNotNull();
        assertThat(bean.getId()).isEqualTo("foo");
        assertThat(bean.getClazz()).isEqualTo("java.lang.String");

        bean = beans.get(1);
        assertThat(bean).isNotNull();
        assertThat(bean.getId()).isEqualTo("bar");
        assertThat(bean.getClazz()).isEqualTo("java.lang.String");
    }

    private void assertThatReportIsCorrect(JobReport jobReport) {
        assertThat(jobReport).isNotNull();
        assertThat(jobReport.getMetrics().getReadCount()).isEqualTo(2);
        assertThat(jobReport.getMetrics().getErrorCount()).isEqualTo(0);
        assertThat(jobReport.getMetrics().getFilterCount()).isEqualTo(0);
        assertThat(jobReport.getMetrics().getWriteCount()).isEqualTo(2);
        assertThat(jobReport.getStatus()).isEqualTo(JobStatus.COMPLETED);
    }

    private InputStream getDataSource(String name) {
        return this.getClass().getResourceAsStream(name);
    }

}
