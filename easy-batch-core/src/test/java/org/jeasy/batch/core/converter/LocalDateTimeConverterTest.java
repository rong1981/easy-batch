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
package org.jeasy.batch.core.converter;

import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LocalDateTimeConverterTest {

	private TypeConverter<String, LocalDateTime> converter = new LocalDateTimeConverter();

	@Test
	public void whenInputValueIsValid_ThenShouldReturnValidLocalDateTime() {
		String dateTime = "2020-01-20T10:15:20";
		LocalDateTime expectedDateTime = LocalDateTime.of(2020, 1, 20, 10, 15, 20);

		LocalDateTime convertedDateTime = converter.convert(dateTime);
		assertThat(convertedDateTime).isEqualTo(expectedDateTime);
	}

	@Test(expected = DateTimeParseException.class)
	public void whenValueIsInvalid_ThenShouldThrowADateTimeParseException() {
		converter.convert("foo");
	}
}
