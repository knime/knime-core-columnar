package org.knime.core.columnar;

import java.io.IOException;
import java.util.List;

import org.knime.core.columnar.chunk.ColumnDataWriter;

public class TestColumnDataWriter implements ColumnDataWriter {

	public List<Double[][]> m_chunks;

	public TestColumnDataWriter(List<Double[][]> chunks) {
		m_chunks = chunks;
	}

	@Override
	public void close() throws Exception {
		m_chunks = null;
	}

	@Override
	public void write(ColumnData[] record) throws IOException {
		final Double[][] data = new Double[record.length][];
		for (int i = 0; i < data.length; i++) {
			data[i] = ((TestDoubleColumnData) record[i]).get();
		}

		m_chunks.add(data);
	}

}
