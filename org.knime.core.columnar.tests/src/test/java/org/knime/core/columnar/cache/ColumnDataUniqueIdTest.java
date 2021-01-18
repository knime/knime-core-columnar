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
 *   18 Jan 2021 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.columnar.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.knime.core.columnar.TestColumnStoreUtils.createDefaultTestColumnStore;

import org.junit.Test;
import org.knime.core.columnar.store.ColumnReadStore;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class ColumnDataUniqueIdTest {

    @SuppressWarnings("resource")
    @Test
    public void testGetters() {
        final ColumnReadStore store = createDefaultTestColumnStore();
        final int columnIndex = Integer.MIN_VALUE;
        final int chunkIndex = Integer.MAX_VALUE;
        final ColumnDataUniqueId cduid = new ColumnDataUniqueId(store, columnIndex, chunkIndex);
        assertEquals(store, cduid.getStore());
        assertEquals(columnIndex, cduid.getColumnIndex());
        assertEquals(chunkIndex, cduid.getChunkIndex());
    }

    @Test
    public void testEqualsReflexive() {
        @SuppressWarnings("resource")
        final ColumnDataUniqueId cduid = new ColumnDataUniqueId(createDefaultTestColumnStore(), 42, 42);
        assertTrue(cduid.equals(cduid)); // NOSONAR
    }

    @Test
    public void testEqualsSymmetric() {
        @SuppressWarnings("resource")
        final ColumnReadStore store = createDefaultTestColumnStore();
        final ColumnDataUniqueId cduid1 = new ColumnDataUniqueId(store, 42, 42);
        final ColumnDataUniqueId cduid2 = new ColumnDataUniqueId(store, 42, 42);
        assertEquals(cduid1, cduid2);
        assertEquals(cduid2, cduid1);
    }

    @Test
    public void testEqualsTransitive() {
        @SuppressWarnings("resource")
        final ColumnReadStore store = createDefaultTestColumnStore();
        final ColumnDataUniqueId cduid1 = new ColumnDataUniqueId(store, 42, 42);
        final ColumnDataUniqueId cduid2 = new ColumnDataUniqueId(store, 42, 42);
        final ColumnDataUniqueId cduid3 = new ColumnDataUniqueId(store, 42, 42);
        assertEquals(cduid1, cduid2);
        assertEquals(cduid2, cduid3);
        assertEquals(cduid1, cduid3);
    }

    @Test
    public void testEqualsConsistent() {
        @SuppressWarnings("resource")
        final ColumnReadStore store = createDefaultTestColumnStore();
        final ColumnDataUniqueId cduid1 = new ColumnDataUniqueId(store, 42, 42);
        final ColumnDataUniqueId cduid2 = new ColumnDataUniqueId(store, 42, 42);
        assertEquals(cduid1.equals(cduid2), cduid1.equals(cduid2));
    }

    @Test
    public void testEqualsNull() {
        @SuppressWarnings("resource")
        final ColumnDataUniqueId cduid = new ColumnDataUniqueId(createDefaultTestColumnStore(), 42, 42);
        assertFalse(cduid.equals(null)); // NOSONAR
    }

    @Test
    public void testEqualsFalse() {
        @SuppressWarnings("resource")
        final ColumnReadStore testStore1 = createDefaultTestColumnStore();
        @SuppressWarnings("resource")
        final ColumnReadStore testStore2 = createDefaultTestColumnStore();
        assertNotEquals(new ColumnDataUniqueId(testStore1, 0, 0), new Object());
        assertNotEquals(new ColumnDataUniqueId(testStore1, 0, 0), new ColumnDataUniqueId(testStore2, 0, 0));
        assertNotEquals(new ColumnDataUniqueId(testStore1, 0, 0), new ColumnDataUniqueId(testStore1, 1, 0));
        assertNotEquals(new ColumnDataUniqueId(testStore1, 0, 0), new ColumnDataUniqueId(testStore1, 0, 1));
    }

    @Test
    public void testHashCodeConsistent() {
        @SuppressWarnings("resource")
        final ColumnDataUniqueId cduid = new ColumnDataUniqueId(createDefaultTestColumnStore(), 42, 42);
        assertEquals(cduid.hashCode(), cduid.hashCode());
    }

    @Test
    public void testHashCodeConsistentWithEquals() {
        @SuppressWarnings("resource")
        final ColumnReadStore store = createDefaultTestColumnStore();
        final ColumnDataUniqueId cduid1 = new ColumnDataUniqueId(store, 42, 42);
        final ColumnDataUniqueId cduid2 = new ColumnDataUniqueId(store, 42, 42);
        assertEquals(cduid1.hashCode(), cduid2.hashCode());
    }

    @Test
    public void testToString() {
        @SuppressWarnings("resource")
        final ColumnReadStore store = createDefaultTestColumnStore();
        final int columnIndex = Integer.MIN_VALUE;
        final int chunkIndex = Integer.MAX_VALUE;
        final ColumnDataUniqueId cduid = new ColumnDataUniqueId(store, columnIndex, chunkIndex);
        assertEquals(String.join(",", store.toString(), Integer.toString(columnIndex), Integer.toString(chunkIndex)),
            cduid.toString());
    }

}
