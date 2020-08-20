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
package org.knime.core.columnar.array;

import org.knime.core.columnar.data.StringData;

// TODO we may want to store sparse arrays etc differently.
class StringArrayChunk extends AbstractArrayChunk<String[]> implements StringData {

    private int m_stringLengthTotal = 0;

    private int m_sizeOfIndex = 0;

    // Read case
    public StringArrayChunk(final int capacity) {
        super(new String[capacity], capacity);
    }

    @Override
    public String getString(final int index) {
        return m_array[index];
    }

    @Override
    public void setString(final int index, final String val) {
        m_array[index] = val;
    }

    @Override
    public void setMissing(final int index) {
        m_array[index] = null;
    }

    @Override
    public boolean isMissing(final int index) {
        return m_array[index] == null;
    }

    @Override
    public void ensureCapacity(final int capacity) {
        // TODO Auto-generated method stub

    }

    @Override
    public int sizeOf() {
        for (; m_sizeOfIndex < getNumValues(); m_sizeOfIndex++) {
            m_stringLengthTotal += getString(m_sizeOfIndex).length();
        }
        // for a 64 bit VM with more than 32 GB heap space:
        // 24 bytes for String array object
        // 8 bytes per reference on String
        // 32 bytes per String object (including held character array)
        // 2 bytes per character
        return 24 + 8 * getMaxCapacity() + 32 * getNumValues() + m_stringLengthTotal * 2;
    }

}
