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
package org.knime.core.columnar;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.chunk.ColumnSelection;

public class TestColumnStore implements ColumnStore {

    private final int m_maxDataCapacity;

    private final ColumnStoreSchema m_schema;

    public List<Double[][]> m_chunks;

    private List<TestDoubleColumnData[]> m_tracker;

    public TestColumnStore(final ColumnStoreSchema schema, final int maxDataCapacity) {
        m_schema = schema;
        m_maxDataCapacity = maxDataCapacity;
        m_chunks = new ArrayList<Double[][]>();
        m_tracker = new ArrayList<TestDoubleColumnData[]>();
    }

    @Override
    public ColumnDataWriter getWriter() {
        return new TestColumnDataWriter(m_chunks);
    }

    @Override
    public ColumnDataFactory getFactory() {
        return new TestColumnDataFactory(m_schema.getNumColumns(), m_maxDataCapacity);
    }

    @Override
    public void saveToFile(final File f) throws IOException {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public ColumnDataReader createReader(final ColumnSelection config) {
        final TestColumnDataReader reader = new TestColumnDataReader(m_chunks, m_maxDataCapacity);
        return new ColumnDataReader() {

            @Override
            public void close() {
                reader.close();
            }

            @Override
            public ColumnData[] read(final int chunkIndex) throws IOException {
                final ColumnData[] data = reader.read(chunkIndex);
                m_tracker.add((TestDoubleColumnData[])data);
                return data;
            }

            @Override
            public int getNumChunks() {
                return reader.getNumChunks();
            }

            @Override
            public int getMaxDataCapacity() {
                return reader.getMaxDataCapacity();
            }
        };
    }

    @Override
    public void close() {
        m_chunks = null;

        // check if all memory has been released before closing this store.
        for (TestDoubleColumnData[] chunk : m_tracker) {
            for (TestDoubleColumnData data : chunk) {
                if (!data.isClosed()) {
                    throw new IllegalStateException("Data not closed.");
                }
            }
        }
    }

    @Override
    public ColumnStoreSchema getSchema() {
        return m_schema;
    }

}
