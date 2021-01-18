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

import java.util.Arrays;

import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ColumnWriteData;
import org.knime.core.columnar.data.StructData.StructReadData;
import org.knime.core.columnar.data.StructData.StructWriteData;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public final class TestStructData extends TestData implements StructWriteData, StructReadData {

    public static final class TestStructDataFactory implements TestDataFactory {

        private final TestDataFactory[] m_inner;

        public TestStructDataFactory(final TestDataFactory... inner) {
            m_inner = inner;
        }

        @Override
        public TestStructData createWriteData(final int capacity) {
            return new TestStructData(
                Arrays.stream(m_inner).map(f -> f.createWriteData(capacity)).toArray(TestData[]::new), false);
        }

        @Override
        public TestStructData createReadData(final Object data) {
            return new TestStructData((TestData[])data, true);
        }

    }

    TestStructData(final TestData[] structs, final boolean close) {
        super(structs, structs[0].capacity());
        if (close) {
            close(structs[0].length());
        }
    }

    @Override
    public StructReadData close(final int length) {
        closeInternal(length);
        return this;
    }

    @Override
    public long sizeOf() {
        return Arrays.stream(get()).map(o -> (TestData)(o)).mapToLong(TestData::sizeOf).sum();
    }

    @Override
    public void expand(final int minimumCapacity) {
        Arrays.stream(get()).map(o -> (TestData)(o)).forEach(t -> t.expand(minimumCapacity));
    }

    @Override
    public synchronized void setMissing(final int index) {
        Arrays.stream(get()).map(o -> (TestData)(o)).forEach(t -> t.setMissing(index));
    }

    @Override
    public synchronized boolean isMissing(final int index) {
        return ((ColumnReadData)get()[0]).isMissing(index);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <C extends ColumnReadData> C getReadDataAt(final int index) {
        return (C)get()[index];
    }

    @SuppressWarnings("unchecked")
    @Override
    public <C extends ColumnWriteData> C getWriteDataAt(final int index) {
        return (C)get()[index];
    }

}
