/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 */
package org.knime.core.columnar.arrow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.knime.core.columnar.arrow.data.ArrowDoubleData;
import org.knime.core.columnar.batch.Batch;
import org.knime.core.columnar.batch.DefaultBatch;
import org.knime.core.columnar.data.ColumnData;
import org.knime.core.columnar.data.ColumnDataSpec;
import org.knime.core.columnar.store.ColumnStoreSchema;

public class ArrowIOTest extends AbstractArrowTest {

    private RootAllocator m_alloc;

    private ColumnStoreSchema m_schema;

    @Before
    public void init() {
        m_alloc = new RootAllocator();
        m_schema = createWideSchema(ColumnDataSpec.doubleSpec(), 15);
    }

    @After
    public void after() {
        m_alloc.close();
    }

    @Test
    public void testShortChunkIO() throws Exception {
        File tmp = createTmpFile();
        ColumnStoreSchema schema = createSchema(ColumnDataSpec.doubleSpec());
        ArrowColumnDataWriter writer = new ArrowColumnDataWriter(schema, tmp, m_alloc, 1024);

        final ArrowDoubleData data = new ArrowDoubleData(m_alloc, 1024);
        data.close(1024);
        writer.write(new DefaultBatch(schema, new ColumnData[]{data}, 1024));
        data.release();

        final ArrowDoubleData dataShort = new ArrowDoubleData(m_alloc, 1024);
        dataShort.close(42);
        writer.write(new DefaultBatch(schema, new ColumnData[]{dataShort}, 42));
        dataShort.release();
        writer.close();

        ArrowColumnDataReader reader = new ArrowColumnDataReader(schema, tmp, m_alloc, createSelection(schema));
        ColumnData read = reader.readRetained(0).get(0);
        assertEquals(1024, read.length());
        System.out.println(read.sizeOf());
        read.release();

        ColumnData readShort = reader.readRetained(1).get(0);
        assertEquals(42, readShort.length());
        readShort.release();

        reader.close();
        tmp.delete();
    }

    @Test
    public void testIOWithMissing() throws Exception {
        File tmp = createTmpFile();
        ArrowColumnDataWriter writer = new ArrowColumnDataWriter(m_schema, tmp, m_alloc, 1024);

        for (int c = 0; c < 32; c++) {
            final ArrowDoubleData[] data = new ArrowDoubleData[m_schema.getNumColumns()];
            for (int i = 0; i < m_schema.getNumColumns(); i++) {
                data[i] = new ArrowDoubleData(m_alloc, 1024);
                for (int j = 0; j < 1024; j++) {
                    if (j % 13 == 0) {
                        data[i].setMissing(j);
                    } else {
                        data[i].setDouble(j, c * j);
                    }
                }
                data[i].close(1024);
            }
            writer.write(new DefaultBatch(m_schema, data, 1024));
            for (ArrowDoubleData d : data) {
                d.release();
            }
        }
        writer.close();

        // read all
        ArrowColumnDataReader reader = new ArrowColumnDataReader(m_schema, tmp, m_alloc, createSelection(m_schema));
        for (int c = 0; c < 32; c++) {
            testRead(reader, c);
        }

        // random access
        testRead(reader, 13);

        // clean-up
        reader.close();
        assertTrue(m_alloc.getAllocatedMemory() == 0);

        // selection
        final ArrowColumnDataReader filteredReader =
            new ArrowColumnDataReader(m_schema, tmp, m_alloc, createSelection(m_schema, 9, 13));
        Batch filteredData = filteredReader.readRetained(13);
        for (int i = 0; i < m_schema.getNumColumns(); i++) {
            if (i == 9 || i == 13) {
                assertNotNull(filteredData.get(i));
                filteredData.get(i).release();
            } else {
                final int finalI = i;
                assertThrows(NoSuchElementException.class, () -> filteredData.get(finalI));
            }
        }
        filteredReader.close();
        tmp.delete();
    }

    private void testRead(final ArrowColumnDataReader reader, final int c) throws IOException {
        Batch dataChunk = reader.readRetained(c);

        for (int i = 0; i < m_schema.getNumColumns(); i++) {
            assertEquals(1024, dataChunk.get(i).length());
            assertTrue(dataChunk.get(i) instanceof ArrowDoubleData);
            for (int j = 0; j < dataChunk.get(i).length(); j++) {
                if (j % 13 == 0) {
                    assertTrue(((ArrowDoubleData)dataChunk.get(i)).isMissing(j));
                } else {
                    assertFalse(((ArrowDoubleData)dataChunk.get(i)).isMissing(j));
                    assertEquals(j * c, ((ArrowDoubleData)dataChunk.get(i)).getDouble(j), 0);
                }
            }
            dataChunk.get(i).release();
        }
    }

}
