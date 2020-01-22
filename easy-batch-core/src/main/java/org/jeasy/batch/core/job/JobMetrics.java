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
package org.jeasy.batch.core.job;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Metrics of a job.
 *
 * @author Mahmoud Ben Hassine (mahmoud.benhassine@icloud.com)
 */
public class JobMetrics implements Serializable {

    private LocalDateTime startTime;

    private LocalDateTime endTime;

    private long readCount;

    private long writeCount;

    private long filterCount;

    private long errorCount;

    private Map<String, Object> customMetrics = new HashMap<>();

    public void incrementFilterCount() {
        filterCount++;
    }

    public void incrementFilterCount(long count) {
        filterCount += count;
    }

    public void incrementErrorCount() {
        errorCount++;
    }

    public void incrementErrorCount(long count) {
        errorCount += count;
    }

    public void incrementReadCount() {
        readCount++;
    }

    public void incrementReadCount(long count) {
        readCount += count;
    }

    public void incrementWriteCount(long count) {
        writeCount += count;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }

    public LocalDateTime getEndTime() {
        return endTime;
    }

    public void setEndTime(LocalDateTime endTime) {
        this.endTime = endTime;
    }

    public Duration getDuration() {
        if (getStartTime() != null && getEndTime() != null) {
            return Duration.between(getStartTime(), getEndTime());
        }
        return null;
    }

    public long getFilterCount() {
        return filterCount;
    }

    public long getErrorCount() {
        return errorCount;
    }

    public long getReadCount() {
        return readCount;
    }

    public long getWriteCount() {
        return writeCount;
    }

    public void addMetric(String name, Object value) {
        customMetrics.put(name, value);
    }

    public Map<String, Object> getCustomMetrics() {
        return customMetrics;
    }
}
