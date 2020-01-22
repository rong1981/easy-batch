package org.jeasy.batch.tutorials.intermediate.csv2xml;

import java.io.IOException;
import java.io.OutputStreamWriter;

import org.jeasy.batch.core.writer.FileRecordWriter;

public class FooterWriter implements FileRecordWriter.FooterCallback {

	@Override
	public void writeFooter(OutputStreamWriter writer) throws IOException {
		writer.write("</tweets>");
	}

}
