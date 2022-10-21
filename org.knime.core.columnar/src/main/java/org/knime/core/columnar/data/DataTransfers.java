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
 *   Oct 11, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.data;

import java.util.Arrays;

import org.knime.core.columnar.data.BooleanData.BooleanReadData;
import org.knime.core.columnar.data.BooleanData.BooleanWriteData;
import org.knime.core.columnar.data.ByteData.ByteReadData;
import org.knime.core.columnar.data.ByteData.ByteWriteData;
import org.knime.core.columnar.data.DoubleData.DoubleReadData;
import org.knime.core.columnar.data.DoubleData.DoubleWriteData;
import org.knime.core.columnar.data.FloatData.FloatReadData;
import org.knime.core.columnar.data.FloatData.FloatWriteData;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.columnar.data.ListData.ListReadData;
import org.knime.core.columnar.data.ListData.ListWriteData;
import org.knime.core.columnar.data.LongData.LongReadData;
import org.knime.core.columnar.data.LongData.LongWriteData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.columnar.data.StructData.StructReadData;
import org.knime.core.columnar.data.StructData.StructWriteData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
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

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class DataTransfers {

    private DataTransfers() {
        //
    }

    public interface DataTransfer {
        void transfer(NullableReadData readData, int readIdx, NullableWriteData writeData, int writeIdx);
    }

    public static DataTransfer[] createTransfers(final ColumnarSchema schema) {
        return schema.specStream()//
            .map(DataTransfers::createTransfer)//
            .toArray(DataTransfer[]::new);
    }

    public static DataTransfer createTransfer(final DataSpec dataSpec) {
        return dataSpec.accept(DataSpecToTransferMapper.INSTANCE);
    }

    private enum DataSpecToTransferMapper implements DataSpec.Mapper<DataTransfer> {

            INSTANCE;

        @Override
        public DataTransfer visit(final BooleanDataSpec spec) {
            return new BooleanDataTransfer();
        }

        @Override
        public DataTransfer visit(final ByteDataSpec spec) {
            return new ByteDataTransfer();
        }

        @Override
        public DataTransfer visit(final DoubleDataSpec spec) {
            return new DoubleDataTransfer();
        }

        @Override
        public DataTransfer visit(final FloatDataSpec spec) {
            return new FloatDataTransfer();
        }

        @Override
        public DataTransfer visit(final IntDataSpec spec) {
            return new IntDataTransfer();
        }

        @Override
        public DataTransfer visit(final LongDataSpec spec) {
            return new LongDataTransfer();
        }

        @Override
        public DataTransfer visit(final VarBinaryDataSpec spec) {
            return new VarBinaryDataTransfer();
        }

        @Override
        public DataTransfer visit(final VoidDataSpec spec) {
            return VoidDataTransfer.INSTANCE;
        }

        @Override
        public DataTransfer visit(final StructDataSpec spec) {
            return new StructDataTransfer(spec);
        }

        @Override
        public DataTransfer visit(final ListDataSpec listDataSpec) {
            return new ListDataTransfer(listDataSpec);
        }

        @Override
        public DataTransfer visit(final StringDataSpec spec) {
            return new StringDataTransfer();
        }

    }

    private enum VoidDataTransfer implements DataTransfer {
            INSTANCE;

        @Override
        public void transfer(final NullableReadData readData, final int readIdx, final NullableWriteData writeData,
            final int writeIdx) {
            // no data to transfer
        }

    }

    private abstract static class AbstractDataTransfer<R extends NullableReadData, W extends NullableWriteData>
        implements DataTransfer {

        @Override
        public final void transfer(final NullableReadData readData, final int readIdx,
            final NullableWriteData writeData, final int writeIdx) {
            if (readData.isMissing(readIdx)) {
                writeData.setMissing(writeIdx);
            } else {
                transferNonMissing((R)readData, readIdx, (W)writeData, writeIdx);
            }
        }

        protected abstract void transferNonMissing(final R readData, int readIdx, final W writeData, int writeIdx);
    }

    private static final class BooleanDataTransfer extends AbstractDataTransfer<BooleanReadData, BooleanWriteData> {

        @Override
        protected void transferNonMissing(final BooleanReadData readData, final int readIdx,
            final BooleanWriteData writeData, final int writeIdx) {
            writeData.setBoolean(writeIdx, readData.getBoolean(readIdx));
        }

    }

    private static final class DoubleDataTransfer extends AbstractDataTransfer<DoubleReadData, DoubleWriteData> {

        @Override
        protected void transferNonMissing(final DoubleReadData readData, final int readIdx,
            final DoubleWriteData writeData, final int writeIdx) {
            writeData.setDouble(writeIdx, readData.getDouble(readIdx));
        }

    }

    private static final class FloatDataTransfer extends AbstractDataTransfer<FloatReadData, FloatWriteData> {

        @Override
        protected void transferNonMissing(final FloatReadData readData, final int readIdx,
            final FloatWriteData writeData, final int writeIdx) {
            writeData.setFloat(writeIdx, readData.getFloat(readIdx));
        }

    }

    private static final class ByteDataTransfer extends AbstractDataTransfer<ByteReadData, ByteWriteData> {

        @Override
        protected void transferNonMissing(final ByteReadData readData, final int readIdx, final ByteWriteData writeData,
            final int writeIdx) {
            writeData.setByte(writeIdx, readData.getByte(readIdx));
        }

    }

    private static final class IntDataTransfer extends AbstractDataTransfer<IntReadData, IntWriteData> {

        @Override
        protected void transferNonMissing(final IntReadData readData, final int readIdx, final IntWriteData writeData,
            final int writeIdx) {
            writeData.setInt(writeIdx, readData.getInt(readIdx));
        }

    }

    private static final class StringDataTransfer extends AbstractDataTransfer<StringReadData, StringWriteData> {

        @Override
        protected void transferNonMissing(final StringReadData readData, final int readIdx,
            final StringWriteData writeData, final int writeIdx) {
            writeData.setBytes(writeIdx, readData.getBytes(readIdx));
        }

    }

    private static final class LongDataTransfer extends AbstractDataTransfer<LongReadData, LongWriteData> {

        @Override
        protected void transferNonMissing(final LongReadData readData, final int readIdx, final LongWriteData writeData,
            final int writeIdx) {
            writeData.setLong(writeIdx, readData.getLong(readIdx));
        }

    }

    private static final class VarBinaryDataTransfer
        extends AbstractDataTransfer<VarBinaryReadData, VarBinaryWriteData> {

        @Override
        protected void transferNonMissing(final VarBinaryReadData readData, final int readIdx,
            final VarBinaryWriteData writeData, final int writeIdx) {
            writeData.setBytes(writeIdx, readData.getBytes(readIdx));
        }

    }

    private static final class StructDataTransfer extends AbstractDataTransfer<StructReadData, StructWriteData> {

        private final DataTransfer[] m_fieldTransfers;

        StructDataTransfer(final StructDataSpec spec) {
            m_fieldTransfers = new DataTransfer[spec.size()];
            Arrays.setAll(m_fieldTransfers, i -> createTransfer(spec.getDataSpec(i)));
        }

        @Override
        protected void transferNonMissing(final StructReadData readData, final int readIdx,
            final StructWriteData writeData, final int writeIdx) {
            for (int i = 0; i < m_fieldTransfers.length; i++) {
                m_fieldTransfers[i].transfer(readData.getReadDataAt(i), readIdx, writeData.getWriteDataAt(i), writeIdx);
            }
        }

    }

    private static final class ListDataTransfer extends AbstractDataTransfer<ListReadData, ListWriteData> {

        private final DataTransfer m_elementTransfer;

        ListDataTransfer(final ListDataSpec spec) {
            m_elementTransfer = createTransfer(spec.getInner());
        }

        @Override
        protected void transferNonMissing(final ListReadData readData, final int readIdx, final ListWriteData writeData,
            final int writeIdx) {
            var elementReadData = readData.createReadData(readIdx);
            var size = elementReadData.length();
            var elementWriteData = writeData.createWriteData(writeIdx, size);
            for (int i = 0; i < size; i++) {
                m_elementTransfer.transfer(elementReadData, i, elementWriteData, writeIdx);
            }
        }

    }

}
