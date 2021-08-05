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
 *   Jun 28, 2021 (chaubold): created
 */
package org.knime.core.columnar.data.dictencoding;

import java.io.DataInput;
import java.io.DataOutput;

import org.knime.core.columnar.ReadData;
import org.knime.core.columnar.WriteData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedReadData;
import org.knime.core.columnar.data.dictencoding.DictEncodedData.DictEncodedWriteData;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;

/**
 * Class holding {@link WriteData} and {@link ReadData} for data holding dictionary encoded
 * elements.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
public final class DictEncodedVarBinaryData {

    private DictEncodedVarBinaryData() {
    }

    /**
     * {@link WriteData} that stores objects using a dictionary encoding
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public interface DictEncodedVarBinaryWriteData extends DictEncodedWriteData {
        /**
         * Set the dictionary entry for a given index.
         * Objects MUST BE ADDED BY INCREASING INDEX, or the offset buffer will be corrupted... meh.
         * @param <T> The type of the dictionary entry
         * @param dictionaryIndex The dictionary index
         * @param dictEntry The dictionary entry that will be inserted at the specified index
         * @param serializer A serializer for the object into a {@link DataOutput}.
         */
        public <T> void setDictEntry(final int dictionaryIndex, final T dictEntry, final ObjectSerializer<T> serializer);

        @Override
        public DictEncodedVarBinaryReadData close(int length);
    }

    /**
     * {@link ReadData} holding dictionary encoded objects.
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public interface DictEncodedVarBinaryReadData extends DictEncodedReadData {
        /**
         * Return the dictionary entry for a given index.
         * @param <T> The type of the dictionary entry
         * @param dictionaryIndex The dictionary index
         * @param deserializer A deserializer for the object from a {@link DataInput}.
         * @return the dictionary entry
         */
        public <T> T getDictEntry(final int dictionaryIndex, final ObjectDeserializer<T> deserializer);
    }

}
