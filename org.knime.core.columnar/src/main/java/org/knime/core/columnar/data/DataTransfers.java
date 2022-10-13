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

import org.knime.core.columnar.data.BooleanData.BooleanReadData;
import org.knime.core.columnar.data.BooleanData.BooleanWriteData;
import org.knime.core.columnar.data.DoubleData.DoubleReadData;
import org.knime.core.columnar.data.DoubleData.DoubleWriteData;
import org.knime.core.columnar.data.IntData.IntReadData;
import org.knime.core.columnar.data.IntData.IntWriteData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
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

    private static IllegalStateException notImplemented(final String type) {
        return new IllegalStateException(type + " transfer isn't implemented");
    }

    private enum DataSpecToTransferMapper implements DataSpec.Mapper<DataTransfer> {

            INSTANCE;

        @Override
        public DataTransfer visit(final BooleanDataSpec spec) {
            return new BooleanDataTransfer();
        }

        @Override
        public DataTransfer visit(final ByteDataSpec spec) {
            throw notImplemented("byte");
        }

        @Override
        public DataTransfer visit(final DoubleDataSpec spec) {
            return new DoubleDataTransfer();
        }

        @Override
        public DataTransfer visit(final FloatDataSpec spec) {
            throw notImplemented("float");
        }

        @Override
        public DataTransfer visit(final IntDataSpec spec) {
            return new IntDataTransfer();
        }

        @Override
        public DataTransfer visit(final LongDataSpec spec) {
            throw notImplemented("long");
        }

        @Override
        public DataTransfer visit(final VarBinaryDataSpec spec) {
            throw notImplemented("varbinary");
        }

        @Override
        public DataTransfer visit(final VoidDataSpec spec) {
            throw notImplemented("void");
        }

        @Override
        public DataTransfer visit(final StructDataSpec spec) {
            throw notImplemented("struct");
        }

        @Override
        public DataTransfer visit(final ListDataSpec listDataSpec) {
            throw notImplemented("list");
        }

        @Override
        public DataTransfer visit(final StringDataSpec spec) {
            return new StringDataTransfer();
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

}
