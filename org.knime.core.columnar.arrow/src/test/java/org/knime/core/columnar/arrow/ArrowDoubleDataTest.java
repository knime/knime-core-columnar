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
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.knime.core.columnar.arrow.ArrowSchemaMapperV0.ArrowDoubleDataSpec;
import org.knime.core.columnar.arrow.data.ArrowDoubleData;
import org.knime.core.columnar.batch.DefaultReadBatch;
import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ColumnDataSpec;
import org.knime.core.columnar.data.ColumnWriteData;
import org.knime.core.columnar.store.ColumnStoreSchema;

public class ArrowDoubleDataTest extends AbstractArrowTest {

    private RootAllocator m_alloc;

    private ColumnStoreSchema m_schema;

    @Before
    public void init() {
        m_alloc = new RootAllocator();
        m_schema = createSchema(ColumnDataSpec.doubleSpec());
    }

    @After
    public void after() {
        m_alloc.close();
    }

    @Test
    public void testMapper() {
        ArrowSchemaMapperV0 mapper = ArrowSchemaMapperV0.INSTANCE;
        ArrowColumnDataSpec[] map = mapper.map(m_schema);
        assertTrue(map.length == 1);
        assertTrue(map[0] instanceof ArrowDoubleDataSpec);
        ColumnWriteData data = map[0].createEmpty(m_alloc, 1024);
        assertTrue(data instanceof ArrowDoubleData);
        assertTrue(map[0].wrap(new Float8Vector("Arrow", m_alloc), null) instanceof ArrowDoubleData);
        data.release();
    }

    @Test
    public void testAlloc() {
        ArrowDoubleData data = new ArrowDoubleData(m_alloc, 1024);
        assertTrue(data.get().getValueCapacity() > 1024);

        for (int i = 0; i < 512; i++) {
            data.setDouble(i, i);
        }

        data.close(512);
        assertEquals(512, data.length());

        for (int i = 0; i < 512; i++) {
            assertEquals(i, data.getDouble(i), 0);
        }

        data.release();
    }

    @Test
    public void testSizeOf() {
        ArrowDoubleData data = new ArrowDoubleData(m_alloc, 1024);
        // actual size can be bigger than ensured capacity.
        assertEquals(data.sizeOf(), data.get().getDataBuffer().capacity() + data.get().getValidityBuffer().capacity());
        data.release();
    }

    @Test
    public void testIOWithMissing() throws Exception {
        File tmp = createTmpFile();
        ArrowColumnDataWriter writer = new ArrowColumnDataWriter(m_schema, tmp, m_alloc, 1024);
        ArrowDoubleData data = new ArrowDoubleData(m_alloc, 1024);
        for (int i = 0; i < 1024; i++) {
            if (i % 13 == 0) {
                data.setMissing(i);
            } else {
                data.setDouble(i, i);
            }
        }
        data.close(1024);
        writer.write(new DefaultReadBatch(m_schema, new ColumnReadData[]{data}, 1024));
        data.release();
        writer.close();

        ArrowColumnDataReader reader = new ArrowColumnDataReader(m_schema, tmp, m_alloc, createSelection(m_schema, 0));

        ColumnReadData dataChunk = reader.readRetained(0).get(0);
        assertEquals(1024, dataChunk.length());
        assertTrue(dataChunk instanceof ArrowDoubleData);

        for (int i = 0; i < dataChunk.length(); i++) {
            if (i % 13 == 0) {
                assertTrue(((ArrowDoubleData)dataChunk).isMissing(i));
            } else {
                assertFalse(((ArrowDoubleData)dataChunk).isMissing(i));
                assertEquals(i, ((ArrowDoubleData)dataChunk).getDouble(i), 0);
            }
        }
        dataChunk.release();
        reader.close();
        tmp.delete();
    }

    @Test
    public void testIdentityWithMissing() {
        ArrowDoubleData data = new ArrowDoubleData(m_alloc, 1024);
        for (int i = 0; i < data.capacity(); i++) {
            if (i % 13 == 0) {
                data.setMissing(i);
            } else {
                data.setDouble(i, i);
            }
        }
        data.close(1024);

        for (int i = 0; i < data.length(); i++) {
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
