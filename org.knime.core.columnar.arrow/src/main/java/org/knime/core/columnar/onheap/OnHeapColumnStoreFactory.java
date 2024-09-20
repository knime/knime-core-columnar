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
 *   Sep 20, 2024 (benjamin): created
 */
package org.knime.core.columnar.onheap;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.IntFunction;

import org.knime.core.columnar.arrow.PathBackedFileHandle;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.DefaultWriteBatch;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.filter.ColumnSelection;
import org.knime.core.columnar.onheap.data.OnHeapDoubleData;
import org.knime.core.columnar.onheap.data.OnHeapIntData;
import org.knime.core.columnar.onheap.data.OnHeapStringData;
import org.knime.core.columnar.store.BatchReadStore;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.columnar.store.ColumnStoreFactoryCreator;
import org.knime.core.columnar.store.FileHandle;
import org.knime.core.table.schema.BooleanDataSpec;
import org.knime.core.table.schema.ByteDataSpec;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DoubleDataSpec;
import org.knime.core.table.schema.FloatDataSpec;
import org.knime.core.table.schema.IntDataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.LongDataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec;
import org.knime.core.table.schema.VoidDataSpec;
import org.knime.core.table.schema.traits.DataTraits;
import org.knime.core.table.schema.traits.ListDataTraits;
import org.knime.core.table.schema.traits.StructDataTraits;

/**
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public final class OnHeapColumnStoreFactory implements ColumnStoreFactory {

    @Override
    public BatchStore createStore(final ColumnarSchema schema, final FileHandle fileHandle) {
        return new OnHeapBatchStore(schema, fileHandle);
    }

    @Override
    public BatchReadStore createReadStore(final Path path) {
        return new OnHeapBatchStore(null, new PathBackedFileHandle(path));
    }

    public static final class OnHeapColumnStoreFactoryCreator implements ColumnStoreFactoryCreator {

        @Override
        public ColumnStoreFactory createFactory(final long memoryLimit) {
            // TODO - the memory limit is for off-heap memory but we are on-heap
            return new OnHeapColumnStoreFactory();
        }
    }

    public static final class OnHeapBatchStore implements BatchStore {

        private final ColumnarSchema m_schema;

        private final FileHandle m_fileHandle;

        OnHeapBatchStore(final ColumnarSchema schema, final FileHandle fileHandle) {
            m_schema = schema;
            m_fileHandle = fileHandle;
        }

        @Override
        public BatchWriter getWriter() {
            return new OnHeapBatchWriter(m_schema, m_fileHandle);
        }

        @Override
        public ColumnarSchema getSchema() {
            return m_schema;
        }

        @Override
        public FileHandle getFileHandle() {
            return m_fileHandle;
        }

        @Override
        public RandomAccessBatchReader createRandomAccessReader(final ColumnSelection selection) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        public long[] getBatchBoundaries() {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        public void close() throws IOException {
            // TODO Auto-generated method stub
        }
    }

    public static final class OnHeapBatchWriter implements BatchWriter {

        private final ColumnarSchema m_schema;

        private final FileHandle m_fileHandle;

        OnHeapBatchWriter(final ColumnarSchema schema, final FileHandle fileHandle) {
            m_schema = schema;
            m_fileHandle = fileHandle;
        }

        @Override
        public WriteBatch create(final int capacity) {
            var numColumns = m_schema.numColumns();
            var data = new NullableWriteData[numColumns];
            for (int i = 0; i < numColumns; i++) {
                var specWithTraits = m_schema.getSpecWithTraits(i);
                var dataFactory = specWithTraits.spec().accept(DataFactoryFromSpecs.INSTANCE, specWithTraits.traits());
                data[i] = dataFactory.apply(capacity);
            }
            return new DefaultWriteBatch(data);
        }

        @Override
        public void write(final ReadBatch batch) throws IOException {
            // TODO Auto-generated method stub

        }

        @Override
        public void close() throws IOException {
            // TODO Auto-generated method stub

        }
    }

    public static final class DataFactoryFromSpecs
        implements DataSpec.MapperWithTraits<IntFunction<NullableWriteData>> {

        public static final DataFactoryFromSpecs INSTANCE = new DataFactoryFromSpecs();

        private DataFactoryFromSpecs() {
            // singleton
        }

        @Override
        public IntFunction<NullableWriteData> visit(final BooleanDataSpec spec, final DataTraits traits) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        public IntFunction<NullableWriteData> visit(final ByteDataSpec spec, final DataTraits traits) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        public IntFunction<NullableWriteData> visit(final DoubleDataSpec spec, final DataTraits traits) {
            return OnHeapDoubleData::new;
        }

        @Override
        public IntFunction<NullableWriteData> visit(final FloatDataSpec spec, final DataTraits traits) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        public IntFunction<NullableWriteData> visit(final IntDataSpec spec, final DataTraits traits) {
            return OnHeapIntData::new;
        }

        @Override
        public IntFunction<NullableWriteData> visit(final LongDataSpec spec, final DataTraits traits) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        public IntFunction<NullableWriteData> visit(final VarBinaryDataSpec spec, final DataTraits traits) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        public IntFunction<NullableWriteData> visit(final VoidDataSpec spec, final DataTraits traits) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        public IntFunction<NullableWriteData> visit(final StructDataSpec spec, final StructDataTraits traits) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        public IntFunction<NullableWriteData> visit(final ListDataSpec listDataSpec, final ListDataTraits traits) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        public IntFunction<NullableWriteData> visit(final StringDataSpec spec, final DataTraits traits) {
            // TODO dictionary encoding
            return OnHeapStringData::new;
        }
    }
}
