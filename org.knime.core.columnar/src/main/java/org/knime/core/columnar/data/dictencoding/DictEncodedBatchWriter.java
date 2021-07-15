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

import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.DefaultReadBatch;
import org.knime.core.columnar.batch.DefaultWriteBatch;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.data.dictencoding.AbstractDictDecodedObjectData.AbstractDictDecodedObjectReadData;
import org.knime.core.columnar.data.dictencoding.DictDecodedStringData.DictDecodedStringWriteData;
import org.knime.core.columnar.data.dictencoding.DictDecodedVarBinaryData.DictDecodedVarBinaryWriteData;
import org.knime.core.columnar.data.dictencoding.DictEncodedStringData.DictEncodedStringWriteData;
import org.knime.core.columnar.data.dictencoding.DictEncodedVarBinaryData.DictEncodedVarBinaryWriteData;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec;
import org.knime.core.table.schema.traits.DataTrait;
import org.knime.core.table.schema.traits.DictEncodingTrait;


/**
 * A {@link DictEncodedBatchWriter} wraps a delegate BatchWriter and converts user-facing
 * data objects into their dictionary encoded representation
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public class DictEncodedBatchWriter implements BatchWriter {

    private final BatchWriter m_delegate;

    private final ColumnarSchema m_schema;

    /**
     * Create a batch writer
     * @param delegate The delegate BatchWriter
     * @param schema The schema
     */
    public DictEncodedBatchWriter(final BatchWriter delegate, final ColumnarSchema schema)
    {
        m_delegate = delegate;
        m_schema = schema;
    }

    @Override
    public void close() throws IOException {
        m_delegate.close();
    }

    @Override
    public WriteBatch create(final int capacity) {
        final WriteBatch batch = m_delegate.create(capacity);
        final NullableWriteData[] data = new NullableWriteData[m_schema.numColumns()];

        for (int i = 0; i < data.length; i++) {
            final NullableWriteData d = batch.get(i);

            DictEncodingTrait dictEncodingTrait = (DictEncodingTrait)m_schema.getTraits(i).get(DataTrait.Type.DICT_ENCODING);
            if (dictEncodingTrait != null && dictEncodingTrait.isEnabled()) {
                data[i] = wrapDictEncodedData(i, d);
            } else {
                data[i] = d;
            }

        }

        return new DefaultWriteBatch(data);
    }

    private NullableWriteData wrapDictEncodedData(final int i, final NullableWriteData d) {
        if (m_schema.getSpec(i) instanceof StringDataSpec && !(d instanceof DictDecodedStringWriteData)) {
            if (!(d instanceof DictEncodedStringWriteData)) {
                throw new IllegalArgumentException("Expected DictEncodedStringWriteData to construct DictDecodedStringWriteData");
            }
            return new DictDecodedStringWriteData((DictEncodedStringWriteData)d);
        } else if (m_schema.getSpec(i) instanceof VarBinaryDataSpec  && !(d instanceof DictDecodedVarBinaryWriteData)) {
            if (!(d instanceof DictEncodedVarBinaryWriteData)) {
                throw new IllegalArgumentException("Expected DictEncodedVarBinaryWriteData to construct DictDecodedVarBinaryWriteData");
            }
            return  new DictDecodedVarBinaryWriteData((DictEncodedVarBinaryWriteData)d);
        }

        return d;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void write(final ReadBatch batch) throws IOException {
        // Replace all DictEncoded Data objects by the content that they wrap
        NullableReadData[] data = new NullableReadData[batch.numData()];

        for (int i = 0; i < batch.numData(); i++) {
            data[i] = batch.get(i);
            if (data[i] instanceof AbstractDictDecodedObjectReadData) {
                data[i] = ((AbstractDictDecodedObjectReadData)data[i]).getDelegate();
            }
        }

        m_delegate.write(new DefaultReadBatch(data));
    }

}
