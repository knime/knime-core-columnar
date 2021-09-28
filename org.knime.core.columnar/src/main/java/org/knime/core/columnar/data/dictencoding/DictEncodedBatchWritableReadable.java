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
 *
 * History
 *   Jul 2, 2021 (Carsten Haubold, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.data.dictencoding;

import java.io.IOException;

import org.knime.core.columnar.batch.BatchWritable;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.RandomAccessBatchReadable;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.table.schema.ColumnarSchema;

/**
 * A {@link DictEncodedBatchWritableReadable} wraps a delegate {@link BatchWritable} and
 * {@link RandomAccessBatchReadable}, and wraps created readers and writers in {@link DictEncodedBatchWriter} and
 * {@link DictEncodedRandomAccessBatchReader} to be able to wrap dictionary encoded data coming from the backend and
 * provide plain-looking Data objects to the frontend and vice versa.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public class DictEncodedBatchWritableReadable
    implements BatchWritable, RandomAccessBatchReadable {

    private final RandomAccessBatchReadable m_readDelegate;

    private final DictEncodedBatchWriter m_writer;

    private final DictElementCache m_dictElementCache = new DictElementCache();

    /**
     * Create with writable and readable delegates
     * @param writable The write delegate
     * @param readable The read delegate
     *
     */
    @SuppressWarnings("resource")
    public DictEncodedBatchWritableReadable(final BatchWritable writable, final RandomAccessBatchReadable readable) {
        m_readDelegate = readable;
        m_writer = new DictEncodedBatchWriter(writable.getWriter(), readable.getSchema(), m_dictElementCache);
    }

    @Override
    public BatchWriter getWriter() {
        return m_writer;
    }

    @Override
    public ColumnarSchema getSchema() {
        return m_readDelegate.getSchema();
    }

    @Override
    public RandomAccessBatchReader createRandomAccessReader(final ColumnSelection selection) {
        return new DictEncodedRandomAccessBatchReader(m_readDelegate, selection, m_readDelegate.getSchema(),
            m_dictElementCache);
    }

    @Override
    public void close() throws IOException {
        m_writer.close();
        m_readDelegate.close();
    }

}
