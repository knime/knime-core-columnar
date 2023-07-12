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
 *   Aug 16, 2021 (benjamin): created
 */
package org.knime.core.columnar.arrow;

import static org.junit.Assert.assertEquals;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.knime.core.columnar.arrow.data.ArrowDictEncodedStringData.ArrowDictEncodedStringDataFactory;
import org.knime.core.columnar.arrow.data.ArrowDictEncodedStringData.ArrowDictEncodedStringReadData;
import org.knime.core.columnar.arrow.data.ArrowDictEncodedStringData.ArrowDictEncodedStringWriteData;
import org.knime.core.columnar.data.dictencoding.DictKeys;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait.KeyType;
import org.knime.core.table.schema.traits.DefaultDataTraits;

@SuppressWarnings("javadoc")
public class StructDictEncodingTest {

    private BufferAllocator m_alloc;

    @Before
    public void before() {
        m_alloc = new RootAllocator();
    }

    @After
    public void after() {
        m_alloc.close();
    }

    private static ArrowDictEncodedStringDataFactory createFactory(final KeyType keyType) {
        return new ArrowDictEncodedStringDataFactory(new DefaultDataTraits(new DictEncodingTrait(keyType)));
    }

    @Test
    public void testSimpleDictCreation() {
        final int numRows = 10;
        final var factory = createFactory(KeyType.LONG_KEY);
        @SuppressWarnings("unchecked")
        final var data = (ArrowDictEncodedStringWriteData<Long>)ArrowColumnDataFactory.createWrite(factory, "0",
            m_alloc, numRows);
        data.setKeyGenerator(DictKeys.createAscendingKeyGenerator(KeyType.LONG_KEY));

        data.setString(0, "foo");
        // [<0, "foo">]

        data.setString(1, "bar");
        // [<0, "foo">, <1, "bar">]

        data.setString(2, "foo");
        // [<0, "foo">, <1, "bar">, <0, null>]
        ArrowDictEncodedStringReadData<Long> dataR = data.close(3);
        assertEquals(0, (long)dataR.getDictKey(0));
        assertEquals(1, (long)dataR.getDictKey(1));
        assertEquals(0, (long)dataR.getDictKey(2));

        assertEquals("foo", dataR.getString(0));
        assertEquals("bar", dataR.getString(1));
        assertEquals("foo", dataR.getString(2));
        dataR.release();
    }

    @Test
    public void testRandomAccessRead() {
        final int numRows = 10;
        final var factory = createFactory(KeyType.BYTE_KEY);
        @SuppressWarnings("unchecked")
        final var data = (ArrowDictEncodedStringWriteData<Byte>)ArrowColumnDataFactory.createWrite(factory, "0",
            m_alloc, numRows);
        data.setKeyGenerator(DictKeys.createAscendingKeyGenerator(KeyType.BYTE_KEY));

        data.setString(0, "foo");
        data.setString(1, "bar");
        data.setString(2, "foo");

        ArrowDictEncodedStringReadData<Byte> dataR = data.close(3);
        assertEquals("foo", dataR.getString(2)); // we need to look up the value stored at index 0 for this!
        assertEquals("foo", dataR.getString(0));
        assertEquals("bar", dataR.getString(1));
        dataR.release();
    }

    @Test(expected = IllegalStateException.class)
    public void testKeyOverflow(){
        final int numRows = Byte.MAX_VALUE * 3;
        final var factory = createFactory(KeyType.BYTE_KEY);
        @SuppressWarnings("unchecked")
        final var data = (ArrowDictEncodedStringWriteData<Byte>)ArrowColumnDataFactory.createWrite(factory, "0",
            m_alloc, numRows);
        data.setKeyGenerator(DictKeys.createAscendingKeyGenerator(KeyType.BYTE_KEY));
        int lastWrittenIndex = 0;

        try {
            for (int r = 0; r < numRows; r++) {
                data.setString(r, String.valueOf(r));
                lastWrittenIndex++;
            }
        } finally {
            ArrowDictEncodedStringReadData<Byte> dataR = data.close(lastWrittenIndex);

            for (int r = 0; r < Byte.MAX_VALUE; r++) {
                assertEquals(String.valueOf(r), dataR.getString(r));
            }

            dataR.release();
        }
    }
}
