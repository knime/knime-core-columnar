///*
// * ------------------------------------------------------------------------
// *
// *  Copyright by KNIME AG, Zurich, Switzerland
// *  Website: http://www.knime.com; Email: contact@knime.com
// *
// *  This program is free software; you can redistribute it and/or modify
// *  it under the terms of the GNU General Public License, Version 3, as
// *  published by the Free Software Foundation.
// *
// *  This program is distributed in the hope that it will be useful, but
// *  WITHOUT ANY WARRANTY; without even the implied warranty of
// *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// *  GNU General Public License for more details.
// *
// *  You should have received a copy of the GNU General Public License
// *  along with this program; if not, see <http://www.gnu.org/licenses>.
// *
// *  Additional permission under GNU GPL version 3 section 7:
// *
// *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
// *  Hence, KNIME and ECLIPSE are both independent programs and are not
// *  derived from each other. Should, however, the interpretation of the
// *  GNU GPL Version 3 ("License") under any applicable laws result in
// *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
// *  you the additional permission to use and propagate KNIME together with
// *  ECLIPSE with only the license terms in place for ECLIPSE applying to
// *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
// *  license terms of ECLIPSE themselves allow for the respective use and
// *  propagation of ECLIPSE together with KNIME.
// *
// *  Additional permission relating to nodes for KNIME that extend the Node
// *  Extension (and in particular that are based on subclasses of NodeModel,
// *  NodeDialog, and NodeView) and that only interoperate with KNIME through
// *  standard APIs ("Nodes"):
// *  Nodes are deemed to be separate and independent programs and to not be
// *  covered works.  Notwithstanding anything to the contrary in the
// *  License, the License does not apply to Nodes, you are not required to
// *  license Nodes under the License, and you are granted a license to
// *  prepare and propagate Nodes, in each case even if such Nodes are
// *  propagated with or for interoperation with KNIME.  The owner of a Node
// *  may freely choose the license terms applicable to such Node, including
// *  when such Node is propagated with or for interoperation with KNIME.
// * ---------------------------------------------------------------------
// */
//package org.knime.core.columnar.arrow.data;
//
//import java.io.IOException;
//import java.nio.ByteBuffer;
//
//import org.apache.arrow.memory.BufferAllocator;
//import org.apache.arrow.vector.FieldVector;
//import org.apache.arrow.vector.VarCharVector;
//import org.apache.arrow.vector.dictionary.DictionaryProvider;
//import org.knime.core.columnar.arrow.ArrowColumnDataFactory;
//import org.knime.core.columnar.data.StringData.StringReadData;
//import org.knime.core.columnar.data.StringData.StringWriteData;
//
///**
// * Arrow implementation of {@link StringWriteData} and {@link StringReadData} don't using a dictionary.
// *
// * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
// * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
// */
//public final class ArrowVarCharData extends AbstractVariableWitdthData<VarCharVector>
//    implements StringWriteData, StringReadData {
//
//    private final StringEncodingHelper m_encodingHelper;
//
//    private ArrowVarCharData(final VarCharVector vector) {
//        super(vector);
//        m_encodingHelper = new StringEncodingHelper();
//    }
//
//    @Override
//    public String getString(final int index) {
//        return m_encodingHelper.decode(m_vector.get(index));
//    }
//
//    @Override
//    public void setString(final int index, final String value) {
//        final ByteBuffer encode = m_encodingHelper.encode(value);
//        m_vector.setSafe(index, encode.array(), 0, encode.limit());
//    }
//
//    @Override
//    public StringReadData close(final int length) {
//        m_vector.setValueCount(length);
//        return this;
//    }
//
//    /** Implementation of {@link ArrowColumnDataFactory} for {@link ArrowVarCharData} */
//    public static final class ArrowVarCharDataFactory extends AbstractFieldVectorDataFactory {
//
//        private static final int CURRENT_VERSION = 0;
//
//        /** Singleton instance of {@link ArrowVarCharDataFactory} */
//        public static final ArrowVarCharDataFactory INSTANCE = new ArrowVarCharDataFactory();
//
//        private ArrowVarCharDataFactory() {
//            // Singleton
//        }
//
//        @Override
//        @SuppressWarnings("resource") // Vector resource is handled by AbstractFieldVectorData
//        public ArrowVarCharData createWrite(final BufferAllocator allocator, final int capacity) {
//            final VarCharVector vector = new VarCharVector("VarCharVector", allocator);
//            vector.allocateNew(capacity);
//            return new ArrowVarCharData(vector);
//        }
//
//        @Override
//        public ArrowVarCharData createRead(final FieldVector vector, final DictionaryProvider provider,
//            final int version) throws IOException {
//            if (version == CURRENT_VERSION) {
//                return new ArrowVarCharData((VarCharVector)vector);
//            } else {
//                throw new IOException("Cannot read ArrowVarCharData with version " + version + ". Current version: "
//                    + CURRENT_VERSION + ".");
//            }
//        }
//
//        @Override
//        public int getVersion() {
//            return CURRENT_VERSION;
//        }
//    }
//}
