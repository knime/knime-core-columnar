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
package org.knime.core.columnar.arrow.data;

import java.io.IOException;
import java.util.function.LongSupplier;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.knime.core.columnar.arrow.ArrowColumnDataFactory;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;

/**
 * Arrow implementation of {@link VarBinaryWriteData} and {@link VarBinaryReadData}.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowVarBinaryData extends AbstractVariableWitdthData<VarBinaryVector>
    implements VarBinaryWriteData, VarBinaryReadData {

    private ArrowVarBinaryData(final VarBinaryVector vector) {
        super(vector);
    }

    @Override
    public byte[] getBytes(final int index) {
        return m_vector.get(m_offset + index);
    }

    @Override
    public void setBytes(final int index, final byte[] value) {
        m_vector.setSafe(m_offset + index, value);
    }

    @Override
    public ArrowVarBinaryData close(final int length) {
        closeWithLength(length);
        return this;
    }

    /** Implementation of {@link ArrowColumnDataFactory} for {@link ArrowVarBinaryData} */
    public static final class ArrowVarBinaryDataFactory extends AbstractFieldVectorDataFactory {

        private static final ArrowColumnDataFactoryVersion CURRENT_VERSION = ArrowColumnDataFactoryVersion.version(0);

        /** Singleton instance of {@link ArrowVarBinaryDataFactory} */
        public static final ArrowVarBinaryDataFactory INSTANCE = new ArrowVarBinaryDataFactory();

        private ArrowVarBinaryDataFactory() {
            // Singleton
        }

        @Override
        public Field getField(final String name, final LongSupplier dictionaryIdSupplier) {
            return Field.nullable(name, MinorType.VARBINARY.getType());
        }

        @Override
        public ArrowVarBinaryData createWrite(final FieldVector vector, final LongSupplier dictionaryIdSupplier,
            final BufferAllocator allocator, final int capacity) {
            final VarBinaryVector v = (VarBinaryVector)vector;
            v.allocateNew(capacity);
            return new ArrowVarBinaryData(v);
        }

        @Override
        public ArrowVarBinaryData createRead(final FieldVector vector, final DictionaryProvider provider,
            final ArrowColumnDataFactoryVersion version) throws IOException {
            if (CURRENT_VERSION.equals(version)) {
                return new ArrowVarBinaryData((VarBinaryVector)vector);
            } else {
                throw new IOException("Cannot read ArrowVarBinaryData with version " + version + ". Current version: "
                    + CURRENT_VERSION + ".");
            }
        }

        @Override
        public ArrowColumnDataFactoryVersion getVersion() {
            return CURRENT_VERSION;
        }
    }
}
