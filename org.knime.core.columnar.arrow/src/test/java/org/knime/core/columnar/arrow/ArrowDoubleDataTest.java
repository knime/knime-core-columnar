package org.knime.core.columnar.arrow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.arrow.ArrowSchemaMapperV0.ArrowDoubleDataSpec;
import org.knime.core.columnar.arrow.data.ArrowDoubleData;
import org.knime.core.columnar.data.DoubleData;

public class ArrowDoubleDataTest extends AbstractArrowTest {

	private RootAllocator m_alloc;
	private ColumnStoreSchema m_schema;

	@Before
	public void init() {
		m_alloc = new RootAllocator();
		m_schema = createSchema(new DoubleData.DoubleDataSpec());
	}

	@After
	public void after() {
		m_alloc.close();
	}

	@Test
	public void testMapper() {
		ArrowSchemaMapperV0 mapper = new ArrowSchemaMapperV0();
		ArrowColumnDataSpec<?>[] map = mapper.map(m_schema);
		assertTrue(map.length == 1);
		assertTrue(map[0] instanceof ArrowDoubleDataSpec);
		assertTrue(map[0].createEmpty(m_alloc) instanceof ArrowDoubleData);
		assertTrue(map[0].wrap(new Float8Vector("Arrow", m_alloc), null) instanceof ArrowDoubleData);
	}

	@Test
	public void testAlloc() {
		ArrowDoubleData data = new ArrowDoubleData(m_alloc);
		data.ensureCapacity(1024);
		assertTrue(data.get().getValueCapacity() > 1024);

		for (int i = 0; i < 512; i++) {
			data.setDouble(i, i);
		}

		data.setNumValues(512);
		assertEquals(512, data.getNumValues());

		for (int i = 0; i < 512; i++) {
			assertEquals(i, ((ArrowDoubleData) data).getDouble(i), 0);
		}

		data.release();
	}

	@Test
	public void testSizeOf() {
		ArrowDoubleData data = new ArrowDoubleData(m_alloc);
		data.ensureCapacity(1024);
		// actual size can be bigger than ensured capacity.
		assertEquals(data.sizeOf(), data.get().getDataBuffer().capacity() + data.get().getValidityBuffer().capacity());
		data.release();
	}

	@Test
	public void testIOWithMissing() throws Exception {
		File tmp = createTmpFile();
		ArrowColumnDataWriter writer = new ArrowColumnDataWriter(tmp, m_alloc, 1024);
		ArrowDoubleData data = new ArrowDoubleData(m_alloc);
		data.ensureCapacity(1024);
		for (int i = 0; i < 1024; i++) {
			if (i % 13 == 0) {
				data.setMissing(i);
			} else {
				data.setDouble(i, i);
			}
		}
		data.setNumValues(1024);
		writer.write(new ColumnData[] { data });
		data.release();
		writer.close();

		ArrowColumnDataReader reader = new ArrowColumnDataReader(m_schema, tmp, m_alloc, createSelection(0));

		ColumnData[] dataChunk = reader.read(0);
		assertEquals(1, dataChunk.length);
		assertEquals(1024, dataChunk[0].getNumValues());
		assertTrue(dataChunk[0] instanceof ArrowDoubleData);

		for (int i = 0; i < dataChunk[0].getNumValues(); i++) {
			if (i % 13 == 0) {
				assertTrue(((ArrowDoubleData) dataChunk[0]).isMissing(i));
			} else {
				assertFalse(((ArrowDoubleData) dataChunk[0]).isMissing(i));
				assertEquals(i, ((ArrowDoubleData) dataChunk[0]).getDouble(i), 0);
			}
		}
		dataChunk[0].release();
		reader.close();
		tmp.delete();
	}

	@Test
	public void testIdentityWithMissing() {
		ArrowDoubleData data = new ArrowDoubleData(m_alloc);
		data.ensureCapacity(1024);
		for (int i = 0; i < data.getMaxCapacity(); i++) {
			if (i % 13 == 0) {
				data.setMissing(i);
			} else {
				data.setDouble(i, i);
			}
		}
		data.setNumValues(1024);

		for (int i = 0; i < data.getNumValues(); i++) {
			if (i % 13 == 0) {
				assertTrue(data.isMissing(i));
			} else {
				assertFalse(data.isMissing(i));
				assertEquals(i, data.getDouble(i), 0);
			}
		}
		data.release();
	}

}
