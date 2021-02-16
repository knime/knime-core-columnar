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
 *   15 Dec 2020 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.data.columnar.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.knime.core.data.columnar.domain.TestDataColumnStoreUtils.DEF_CAPACITY;
import static org.knime.core.data.columnar.domain.TestDataColumnStoreUtils.closeWriteReleaseBatches;
import static org.knime.core.data.columnar.domain.TestDataColumnStoreUtils.createBatches;
import static org.knime.core.data.columnar.domain.TestDataColumnStoreUtils.createSpec;
import static org.knime.core.data.columnar.domain.TestDataColumnStoreUtils.readDefaultTable;
import static org.knime.core.data.columnar.domain.TestDataColumnStoreUtils.writeDefaultTable;
import static org.knime.core.data.columnar.domain.TestDataColumnStoreUtils.writeIntegers;
import static org.knime.core.data.columnar.domain.TestDataColumnStoreUtils.writeRowKeys;
import static org.knime.core.data.columnar.domain.TestDataColumnStoreUtils.writeStrings;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.AfterClass;
import org.junit.Test;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.store.BatchFactory;
import org.knime.core.columnar.store.BatchReader;
import org.knime.core.columnar.store.BatchWriter;
import org.knime.core.columnar.store.ColumnStore;
import org.knime.core.columnar.testing.TestColumnStore;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.filestore.internal.NotInWorkflowWriteFileStoreHandler;
import org.knime.core.data.v2.RowKeyType;
import org.knime.core.data.v2.ValueSchema;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class DomainColumnStoreTest {

    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4);

    @AfterClass
    public static void tearDownTests() {
        EXECUTOR.shutdown();
    }

    @SuppressWarnings("resource")
    private static DomainColumnStore generateDomainStore() {
        final DataTableSpec spec = createSpec();
        final ValueSchema valueSchema =
            ValueSchema.create(spec, RowKeyType.CUSTOM, NotInWorkflowWriteFileStoreHandler.create());
        final ColumnarValueSchema schema = ColumnarValueSchemaUtils.create(valueSchema);
        return new DomainColumnStore(TestColumnStore.create(schema), new DefaultDomainStoreConfig(schema, 60, true),
            EXECUTOR);
    }

    @Test()
    public void testDomains() throws IOException {
        try (final DomainColumnStore store = generateDomainStore()) {
            final WriteBatch[] batches = createBatches(store, 4, 4);
            writeRowKeys(batches, "1", "2", "3", "4");
            writeIntegers(batches, 3, 1, 3, 4);
            writeStrings(batches, "3", "1", null, "3");
            closeWriteReleaseBatches(store, batches);
            assertNull(store.getDomain(0));

            final DataColumnDomain intDomain = store.getDomain(1);
            assertEquals(new IntCell(1), intDomain.getLowerBound());
            assertEquals(new IntCell(4), intDomain.getUpperBound());
            assertNull(intDomain.getValues());

            final DataColumnDomain stringDomain = store.getDomain(2);
            assertNull(stringDomain.getLowerBound());
            assertNull(stringDomain.getUpperBound());
            assertEquals(Stream.of(new StringCell("1"), new StringCell("3")).collect(Collectors.toSet()),
                stringDomain.getValues());
        }
    }

    @Test()
    public void testSetMaxPossibleValues() throws IOException {
        try (final DomainColumnStore store = generateDomainStore()) {
            final WriteBatch[] batches = createBatches(store, 2, 2);
            writeRowKeys(batches, "1", "2");
            writeStrings(batches, "1", "2");
            store.setMaxPossibleValues(1);
            closeWriteReleaseBatches(store, batches);

            final DataColumnDomain stringDomain = store.getDomain(2);
            assertNull(stringDomain.getValues());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testSetMaxPossibleValuesIllegalStateException() throws IOException {
        try (final DomainColumnStore store = generateDomainStore()) {
            final WriteBatch[] batches = createBatches(store, 2, 2);
            writeRowKeys(batches, "1", "2");
            writeStrings(batches, "1", "2");
            closeWriteReleaseBatches(store, batches);
            store.setMaxPossibleValues(1);
        }
    }

    @Test
    public void testWriteRead() throws IOException {
        try (final ColumnStore store = generateDomainStore()) {
            writeDefaultTable(store);
            readDefaultTable(store);
            readDefaultTable(store);
        }
    }

    @Test
    public void testFactorySingleton() throws IOException {
        try (final ColumnStore store = generateDomainStore()) {
            assertEquals(store.getFactory(), store.getFactory());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetFactoryAfterWriterClose() throws IOException {
        try (final ColumnStore store = generateDomainStore()) {
            try (final BatchWriter writer = store.getWriter()) { // NOSONAR
            }
            store.getFactory();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetFactoryAfterStoreClose() throws IOException {
        try (final ColumnStore store = generateDomainStore()) {
            store.close(); // NOSONAR
            store.getFactory();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateAfterWriterClose() throws IOException {
        try (final ColumnStore store = generateDomainStore()) {
            final BatchFactory factory = store.getFactory();
            try (final BatchWriter writer = store.getWriter()) { // NOSONAR
            }
            factory.create(DEF_CAPACITY);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateAfterStoreClose() throws IOException {
        try (final ColumnStore store = generateDomainStore()) {
            final BatchFactory factory = store.getFactory();
            store.close(); // NOSONAR
            factory.create(DEF_CAPACITY);
        }
    }

    @Test
    public void testWriterSingleton() throws IOException {
        try (final ColumnStore store = generateDomainStore();
                final BatchWriter writer1 = store.getWriter();
                final BatchWriter writer2 = store.getWriter()) {
            assertEquals(writer1, writer2);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetWriterAfterWriterClose() throws IOException {
        try (final ColumnStore store = generateDomainStore()) {
            try (final BatchWriter writer = store.getWriter()) { // NOSONAR
            }
            try (final BatchWriter writer = store.getWriter()) { // NOSONAR
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetWriterAfterStoreClose() throws IOException {
        try (final ColumnStore store = generateDomainStore()) {
            store.close(); // NOSONAR
            try (final BatchWriter writer = store.getWriter()) { // NOSONAR
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnWriteAfterWriterClose() throws IOException {
        try (final ColumnStore store = generateDomainStore()) {
            store.getWriter().close();
            writeDefaultTable(store);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnWriteAfterStoreClose() throws IOException {
        try (final ColumnStore store = generateDomainStore()) {
            store.close(); // NOSONAR
            writeDefaultTable(store);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnSaveWhileWriterOpen() throws IOException {
        try (final ColumnStore store = generateDomainStore()) {
            store.save(null);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnSaveAfterStoreClose() throws IOException {
        try (final ColumnStore store = generateDomainStore()) {
            writeDefaultTable(store);
            store.close(); // NOSONAR
            store.save(null);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateReaderWhileWriterOpen() throws IOException {
        try (final ColumnStore store = generateDomainStore()) {
            try (final BatchReader reader = store.createReader()) { // NOSONAR
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnCreateReaderAfterStoreClose() throws IOException {
        try (final ColumnStore store = generateDomainStore()) {
            writeDefaultTable(store);
            store.close(); // NOSONAR
            try (final BatchReader reader = store.createReader()) { // NOSONAR
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnReadAfterReaderClose() throws IOException {
        try (final ColumnStore store = generateDomainStore()) {
            writeDefaultTable(store);
            try (final BatchReader reader = store.createReader()) {
                reader.close(); // NOSONAR
                reader.readRetained(0);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnReadAfterStoreClose() throws IOException {
        try (final ColumnStore store = generateDomainStore()) {
            writeDefaultTable(store);
            try (final BatchReader reader = store.createReader()) {
                store.close(); // NOSONAR
                reader.readRetained(0);
            }
        }
    }

}
