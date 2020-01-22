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
package org.jeasy.batch.core.mapper;

import org.jeasy.batch.core.beans.ExtendedPerson;
import org.jeasy.batch.core.beans.Gender;
import org.jeasy.batch.core.beans.Person;
import org.jeasy.batch.core.converter.TypeConverter;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class ObjectMapperTest {

    @Test
    public void whenValuesAreValid_ThenTheMappedObjectShouldBeCorrectlyPopulated() throws Exception {

        ObjectMapper<Person> mapper = new ObjectMapper<>(Person.class);

        // input values
        Map<String, String> values = new HashMap<>();
        values.put("firstName", "foo");
        values.put("lastName", "bar");
        values.put("age", "30");
        values.put("birthDate", "1990-12-12");
        values.put("married", "true");
        values.put("marriageDate", "2010-12-12");

        Person person = mapper.mapObject(values);

        // person bean must be not null
        assertThat(person).isNotNull();

        // person bean must be correctly populated
        assertThat(person.getFirstName()).isEqualTo("foo");
        assertThat(person.getLastName()).isEqualTo("bar");
        assertThat(person.getAge()).isEqualTo(30);
        assertThat(new SimpleDateFormat("yyyy-MM-dd").format(person.getBirthDate())).isEqualTo("1990-12-12");
        assertThat(person.isMarried()).isTrue();
        assertThat(person.getMarriageDate()).isEqualTo(LocalDate.of(2010, 12, 12));
    }

    @Test
    public void whenValuesAreValid_ThenInheritedFieldsOfTheMappedObjectShouldBeCorrectlyPopulated() throws Exception {

        ObjectMapper<ExtendedPerson> mapper = new ObjectMapper<>(ExtendedPerson.class);

        // input values
        Map<String, String> values = new HashMap<>();
        values.put("firstName", "foo");
        values.put("lastName", "bar");
        values.put("age", "30");
        values.put("birthDate", "1990-12-12");
        values.put("married", "true");
        values.put("nickName", "FB");
        values.put("marriageDate", "2010-12-12");

        // map record to extended person bean
        ExtendedPerson extendedPerson = mapper.mapObject(values);

        // extended person bean must be not null
        assertThat(extendedPerson).isNotNull();

        // extended person bean must be correctly populated
        assertThat(extendedPerson.getFirstName()).isEqualTo("foo");
        assertThat(extendedPerson.getLastName()).isEqualTo("bar");
        assertThat(extendedPerson.getAge()).isEqualTo(30);
        assertThat(new SimpleDateFormat("yyyy-MM-dd").format(extendedPerson.getBirthDate())).isEqualTo("1990-12-12");
        assertThat(extendedPerson.isMarried()).isTrue();
        assertThat(extendedPerson.getMarriageDate()).isEqualTo(LocalDate.of(2010, 12, 12));
        assertThat(extendedPerson.getNickName()).isEqualTo("FB");
    }

    @Test
    public void whenACustomTypeConverterIsRegistered_ThenItShouldBeUsedToConvertTheCustomType() throws Exception {

        ObjectMapper<Person> mapper = new ObjectMapper<>(Person.class);
        mapper.registerTypeConverter(new TypeConverter<String, Gender>() {
            @Override
            public Gender convert(String value) {
                return Gender.valueOf(value.toUpperCase());
            }
        });

        Map<String, String> values = new HashMap<>();
        values.put("gender", "MALE");

        Person person = mapper.mapObject(values);

        assertThat(person.getGender()).isNotNull().isEqualTo(Gender.MALE);
    }

    @Test
    public void whenASetterDoesNotExist_ThenShouldLogAWarning() throws Exception {

        ObjectMapper<Person> mapper = new ObjectMapper<>(Person.class);

        Map<String, String> values = new HashMap<>();
        values.put("nickName", "foo");

        try {
            Person person = mapper.mapObject(values);
            assertThat(person).isNotNull();
        } catch (Exception e) {
            fail("Should not throw an exception even if the setter does not exist");
        }
    }

    @Test
    public void whenAttemptingToSetANullValue_ThenShouldNotCallTheSetter() throws Exception {

        ObjectMapper<Person> mapper = new ObjectMapper<>(Person.class);

        Map<String, String> values = new HashMap<>();
        values.put("age", null);

        Person person = mapper.mapObject(values);
        assertThat(person).isNotNull();
        assertThat(person.getAge()).isEqualTo(0);
    }

}



