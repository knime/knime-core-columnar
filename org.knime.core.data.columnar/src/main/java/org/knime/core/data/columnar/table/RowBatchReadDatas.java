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
 *   Oct 13, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table;

import org.apache.commons.lang3.NotImplementedException;
import org.knime.core.columnar.data.DoubleData.DoubleReadData;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataValue;
import org.knime.core.data.columnar.table.ValueExtractors.DoubleValueExtractor;
import org.knime.core.data.columnar.table.ValueExtractors.StringValueExtractor;
import org.knime.core.data.columnar.table.ValueExtractors.ValueExtractor;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.schema.BooleanDataSpec;
import org.knime.core.table.schema.ByteDataSpec;
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

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class RowBatchReadDatas {

    static <W extends WriteAccess> NullableReadData createRowBatchReadData(final int colIdx,
        final ValueFactory<?, W> valueFactory, final DataRow[] rows) {
        var dataSpec = valueFactory.getSpec();
        var valueExtractor = ValueExtractors.createExtractor(valueFactory);
        return dataSpec.accept(RowBatchReadDataFactoryMapper.INSTANCE).create(colIdx, valueExtractor, rows);
    }

    static <W extends WriteAccess> NullableReadData createRowKeyBatchReadData(final ValueFactory<?, W> valueFactory,
        final DataRow[] rows) {
        // TODO add support for other types of row keys once we can have e.g. long-based row keys
        return new RowKeyBatchReadData(rows);
    }

    private interface RowBatchReadDataFactory {
        NullableReadData create(int colIdx, final ValueExtractor extractor, final DataRow[] rows);
    }

    private enum RowBatchReadDataFactoryMapper implements DataSpec.Mapper<RowBatchReadDataFactory> {
            INSTANCE;

        private static NotImplementedException notImplemented(final String type) {
            return new NotImplementedException(type + " RowBatchReadData is not implemented.");
        }

        @Override
        public RowBatchReadDataFactory visit(final BooleanDataSpec spec) {
            throw notImplemented("boolean");
        }

        @Override
        public RowBatchReadDataFactory visit(final ByteDataSpec spec) {
            throw notImplemented("byte");
        }

        @Override
        public RowBatchReadDataFactory visit(final DoubleDataSpec spec) {
            return DoubleRowBatchReadData::new;
        }

        @Override
        public RowBatchReadDataFactory visit(final FloatDataSpec spec) {
            throw notImplemented("float");
        }

        @Override
        public RowBatchReadDataFactory visit(final IntDataSpec spec) {
            throw notImplemented("int");
        }

        @Override
        public RowBatchReadDataFactory visit(final LongDataSpec spec) {
            throw notImplemented("long");
        }

        @Override
        public RowBatchReadDataFactory visit(final VarBinaryDataSpec spec) {
            throw notImplemented("varbinary");
        }

        @Override
        public RowBatchReadDataFactory visit(final VoidDataSpec spec) {
            throw notImplemented("void");
        }

        @Override
        public RowBatchReadDataFactory visit(final StructDataSpec spec) {
            throw notImplemented("struct");
        }

        @Override
        public RowBatchReadDataFactory visit(final ListDataSpec listDataSpec) {
            throw notImplemented("list");
        }

        @Override
        public RowBatchReadDataFactory visit(final StringDataSpec spec) {
            return StringRowBatchReadData::new;
        }

    }

    private abstract static class AbstractRowBatchReadData implements NullableReadData {
        protected final DataRow[] m_rowBatch;

        AbstractRowBatchReadData(final DataRow[] rowBatch) {
            m_rowBatch = rowBatch;
        }

        @Override
        public int length() {
            return m_rowBatch.length;
        }

        @Override
        public void retain() {
            // no reference counting necessary since everything is on heap
        }

        @Override
        public void release() {
            // no reference counting necessary since everything is on heap
        }

        @Override
        public long sizeOf() {
            throw new UnsupportedOperationException();
        }

    }

    private static final class RowKeyBatchReadData extends AbstractRowBatchReadData implements StringReadData {

        RowKeyBatchReadData(final DataRow[] rowBatch) {
            super(rowBatch);
            // TODO Auto-generated constructor stub
        }

        @Override
        public boolean isMissing(final int index) {
            return false;
        }

        @Override
        public String getString(final int index) {
            return m_rowBatch[index].getKey().getString();
        }

        @Override
        public byte[] getBytes(final int index) {
            throw new UnsupportedOperationException();
        }

    }

    private abstract static class ColumnRowBatchReadData<E extends ValueExtractor> extends AbstractRowBatchReadData {

        private final int m_colIdx;

        protected final E m_extractor;

        ColumnRowBatchReadData(final int colIdx, final ValueExtractor extractor, final DataRow[] rowBatch) {
            super(rowBatch);
            m_extractor = (E)extractor;
            m_colIdx = colIdx;
        }

        protected DataValue getValue(final int rowIdx) {
            return m_rowBatch[rowIdx].getCell(m_colIdx);
        }

        @Override
        public boolean isMissing(final int index) {
            return m_rowBatch[index].getCell(m_colIdx).isMissing();
        }

    }

    private static final class StringRowBatchReadData<D extends DataValue>
        extends ColumnRowBatchReadData<StringValueExtractor> implements StringReadData {

        StringRowBatchReadData(final int colIdx, final ValueExtractor extractor, final DataRow[] rowBatch) {
            super(colIdx, extractor, rowBatch);
        }

        @Override
        public String getString(final int index) {
            return m_extractor.getString(getValue(index));
        }

        @Override
        public byte[] getBytes(final int index) {
            return m_extractor.getBytes(getValue(index));
        }

    }

    private static final class DoubleRowBatchReadData extends ColumnRowBatchReadData<DoubleValueExtractor>
        implements DoubleReadData {

        DoubleRowBatchReadData(final int colIdx, final ValueExtractor extractor, final DataRow[] rowBatch) {
            super(colIdx, extractor, rowBatch);
        }

        @Override
        public double getDouble(final int index) {
            return m_extractor.getDouble(getValue(index));
        }

    }

}
