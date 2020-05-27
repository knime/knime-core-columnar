package org.knime.core.columnar.cache;

import java.io.File;
import java.io.IOException;

import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.chunk.ColumnReaderConfig;

public class TestColumnStore implements ColumnStore {

	@Override
	public ColumnDataWriter getWriter() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ColumnDataFactory getFactory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void saveToFile(File f) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ColumnDataReader createReader(ColumnReaderConfig config) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub
	}

	@Override
	public ColumnStoreSchema getSchema() {
		// TODO Auto-generated method stub
		return null;
	}

}
