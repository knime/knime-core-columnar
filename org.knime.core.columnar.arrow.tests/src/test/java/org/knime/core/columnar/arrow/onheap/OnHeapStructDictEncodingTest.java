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
package org.knime.core.columnar.arrow.onheap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowDictEncodedStringData.ArrowDictEncodedStringDataFactory;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowDictEncodedStringData.ArrowDictEncodedStringReadData;
import org.knime.core.columnar.arrow.onheap.data.OnHeapArrowDictEncodedStringData.ArrowDictEncodedStringWriteData;
import org.knime.core.columnar.data.dictencoding.DictKeys;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait.KeyType;
import org.knime.core.table.schema.traits.DefaultDataTraits;

/**
 * Tests for struct based dictionary encoding.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("static-method")
final class OnHeapStructDictEncodingTest {

    private static ArrowDictEncodedStringDataFactory createFactory(final KeyType keyType) {
        return new ArrowDictEncodedStringDataFactory(new DefaultDataTraits(new DictEncodingTrait(keyType)));
    }

    @Test
    void testSimpleDictCreation() {
        final int numRows = 10;
        final var factory = createFactory(KeyType.LONG_KEY);
        @SuppressWarnings("unchecked")
        final var data = (ArrowDictEncodedStringWriteData<Long>)factory.createWrite(numRows);
        data.setKeyGenerator(DictKeys.createAscendingKeyGenerator(KeyType.LONG_KEY));

        data.setString(0, "foo");
        // [<0, "foo">]

        data.setString(1, "bar");
        // [<0, "foo">, <1, "bar">]

        data.setString(2, "foo");
        // [<0, "foo">, <1, "bar">, <0, null>]
        ArrowDictEncodedStringReadData<Long> dataR = data.close(3);
        assertEquals(0, (long)dataR.getDictKey(0), "Unexpected dict key at index 0");
        assertEquals(1, (long)dataR.getDictKey(1), "Unexpected dict key at index 1");
        assertEquals(0, (long)dataR.getDictKey(2), "Unexpected dict key at index 2");

        assertEquals("foo", dataR.getString(0), "Unexpected string value at index 0");
        assertEquals("bar", dataR.getString(1), "Unexpected string value at index 1");
        assertEquals("foo", dataR.getString(2), "Unexpected string value at index 2");
        dataR.release();
    }

    @Test
    void testRandomAccessRead() {
        final int numRows = 10;
        final var factory = createFactory(KeyType.BYTE_KEY);
        @SuppressWarnings("unchecked")
        final var data = (ArrowDictEncodedStringWriteData<Byte>)factory.createWrite(numRows);
        data.setKeyGenerator(DictKeys.createAscendingKeyGenerator(KeyType.BYTE_KEY));

        data.setString(0, "foo");
        data.setString(1, "bar");
        data.setString(2, "foo");

        ArrowDictEncodedStringReadData<Byte> dataR = data.close(3);
        // we need to look up the value stored at index 0 for this!
        assertEquals("foo", dataR.getString(2), "Unexpected string value at index 2");
        assertEquals("foo", dataR.getString(0), "Unexpected string value at index 0");
        assertEquals("bar", dataR.getString(1), "Unexpected string value at index 1");
        dataR.release();
    }

    @Test
    void testKeyOverflow() {
        final int numRows = Byte.MAX_VALUE * 3;
        final var factory = createFactory(KeyType.BYTE_KEY);
        @SuppressWarnings("unchecked")
        final var data = (ArrowDictEncodedStringWriteData<Byte>)factory.createWrite(numRows);
        data.setKeyGenerator(DictKeys.createAscendingKeyGenerator(KeyType.BYTE_KEY));
        int lastWrittenIndex = 0;

        for (int r = 0; r < numRows; r++) {
            if (r < 256) {
                data.setString(r, String.valueOf(r));
                lastWrittenIndex++;
            } else {
                var row = r;
                assertThrows(IllegalStateException.class, () -> data.setString(row, String.valueOf(row)),
                    "Expected IllegalStateException when writing beyond the maximum key range.");
                break;
            }
        }

        var dataR = data.close(lastWrittenIndex + 1);
        for (int r = 0; r < lastWrittenIndex; r++) {
            assertEquals(String.valueOf(r), dataR.getString(r), "Unexpected string value at index " + r);
        }
        dataR.release();
    }
}
