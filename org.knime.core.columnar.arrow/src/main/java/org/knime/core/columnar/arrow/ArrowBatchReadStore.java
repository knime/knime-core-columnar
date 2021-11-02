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

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.BufferAllocator;
import org.knime.core.columnar.arrow.compress.ArrowCompressionUtil;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.filter.DefaultColumnSelection;
import org.knime.core.columnar.store.BatchReadStore;

/**
 * A {@link BatchReadStore} implementation for Arrow.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowBatchReadStore extends AbstractArrowBatchReadable implements BatchReadStore {

    private AtomicInteger m_numBatches;

    private AtomicInteger m_maxLength;

    private Boolean m_useLZ4BlockCompression;

    ArrowBatchReadStore(final Path path, final BufferAllocator allocator) {
        this(path, allocator, null, null);
    }

    ArrowBatchReadStore(final Path path, final BufferAllocator allocator,
        final AtomicInteger numBatches, final AtomicInteger maxLength) {
        super(ArrowSchemaUtils.readSchema(path), path, allocator);
        m_numBatches = numBatches;
        m_maxLength = maxLength;
    }

    @Override
    public ArrowBatchReader createRandomAccessReader(final ColumnSelection config) {
        final ArrowColumnDataFactory[] factories = ArrowSchemaMapper.map(m_schema);
        return new ArrowBatchReader(m_path.toFile(), m_allocator, factories, config);
    }

    private final void initMetadata() {
        try (final ArrowBatchReader reader =
            createRandomAccessReader(new DefaultColumnSelection(m_schema.numColumns()))) {
            m_numBatches = new AtomicInteger(reader.numBatches());
            m_maxLength = new AtomicInteger(reader.maxLength());
            m_useLZ4BlockCompression =
                reader.getMetadata().containsKey(ArrowReaderWriterUtils.ARROW_LZ4_BLOCK_FEATURE_KEY);
        } catch (final IOException e) {
            throw new IllegalStateException("Error when reading footer.", e);
        }
    }

    @Override
    public int numBatches() {
        if (m_numBatches == null) {
            initMetadata();
        }
        return m_numBatches.get();
    }

    @Override
    public int batchLength() {
        if (m_maxLength == null) {
            initMetadata();
        }
        return m_maxLength.get();
    }

    /**
     * @return Whether the store's data was persisted using the deprecated
     *         {@link ArrowCompressionUtil#ARROW_LZ4_BLOCK_COMPRESSION LZ4 block buffer compression} type.
     */
    public boolean isUseLZ4BlockCompression() {
        if (m_useLZ4BlockCompression == null) {
            initMetadata();
        }
        return m_useLZ4BlockCompression;
    }
}
