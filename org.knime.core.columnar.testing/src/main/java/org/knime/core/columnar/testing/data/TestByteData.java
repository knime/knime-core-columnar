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
package org.knime.core.columnar.testing.data;

import org.knime.core.columnar.data.ByteData.ByteReadData;
import org.knime.core.columnar.data.ByteData.ByteWriteData;
import org.knime.core.table.access.ByteAccess.ByteWriteAccess;
import org.knime.core.table.access.WriteAccess;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public final class TestByteData extends AbstractTestData implements ByteWriteData, ByteReadData {

    public static final class TestByteDataFactory implements TestDataFactory {

        public static final TestByteDataFactory INSTANCE = new TestByteDataFactory();

        private TestByteDataFactory() {
        }

        @Override
        public TestByteData createWriteData(final int capacity) {
            return new TestByteData(capacity);
        }

        @Override
        public TestByteData createReadData(final Object[] data) {
            return createReadData(data, data.length);
        }

        @Override
        public TestByteData createReadData(final Object[] data, final int length) {
            return new TestByteData(data, length);
        }

    }

    TestByteData(final int capacity) {
        super(capacity);
    }

    TestByteData(final Object[] bytes, final int length) {
        super(bytes);
        close(length);
    }

    @Override
    public ByteReadData close(final int length) {
        closeInternal(length);
        return this;
    }

    @Override
    public synchronized byte getByte(final int index) {
        return (byte)get()[index];
    }

    @Override
    public synchronized void setByte(final int index, final byte val) {
        get()[index] = val;
    }

    @Override
    public void writeToAccess(final WriteAccess access, final int index) {
        ((ByteWriteAccess)access).setByteValue(getByte(index));
    }
}
