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

import org.jeasy.batch.core.marshaller.RecordMarshaller;
import org.jeasy.batch.core.record.Record;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.PropertyException;
import java.io.StringWriter;

import static org.jeasy.batch.core.util.Utils.checkNotNull;

/**
 * Marshals an object to XML using JAXB.
 * The object must be annotated with JAXB annotations.
 *
 * @param <P> type of the objects to marshal
 * @author Mahmoud Ben Hassine (mahmoud.benhassine@icloud.com)
 */
public class XmlRecordMarshaller<P> implements RecordMarshaller<Record<P>, XmlRecord> {

    private Marshaller marshaller;

    /**
     * Create a new {@link XmlRecordMarshaller}.
     *
     * @param type the type of object to marshal
     * @throws JAXBException if an exception occurs during JAXB context setup
     */
    public XmlRecordMarshaller(final Class<P> type) throws JAXBException {
        checkNotNull(type, "target type");
        JAXBContext jaxbContext = JAXBContext.newInstance(type);
        marshaller = jaxbContext.createMarshaller();
        disableXmlDeclaration();
    }

    /**
     * Create a new {@link XmlRecordMarshaller}.
     *
     * @param jaxbContext a pre-configured JAXB context
     * @throws JAXBException if an exception occurs during Jaxb context setup
     */
    public XmlRecordMarshaller(final JAXBContext jaxbContext) throws JAXBException {
        checkNotNull(jaxbContext, "jaxb context");
        marshaller = jaxbContext.createMarshaller();
        disableXmlDeclaration();
    }

    private void disableXmlDeclaration() throws PropertyException {
        marshaller.setProperty("com.sun.xml.bind.xmlDeclaration", Boolean.FALSE);
    }

    @Override
    public XmlRecord processRecord(final Record<P> record) throws Exception {
        StringWriter stringWriter = new StringWriter();
        marshaller.marshal(record.getPayload(), stringWriter);
        return new XmlRecord(record.getHeader(), stringWriter.toString());
    }
}
