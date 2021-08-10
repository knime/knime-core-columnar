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
import static org.knime.core.data.columnar.TestDataBatchStoreUtils.closeWriteReleaseBatches;
import static org.knime.core.data.columnar.TestDataBatchStoreUtils.createBatches;
import static org.knime.core.data.columnar.TestDataBatchStoreUtils.createSpec;
import static org.knime.core.data.columnar.TestDataBatchStoreUtils.readDefaultTable;
import static org.knime.core.data.columnar.TestDataBatchStoreUtils.writeDefaultTable;
import static org.knime.core.data.columnar.TestDataBatchStoreUtils.writeRowKeys;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.AfterClass;
import org.junit.Test;
import org.knime.core.columnar.batch.BatchWritable;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.columnar.testing.ColumnarTest;
import org.knime.core.columnar.testing.TestBatchStore;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.filestore.internal.NotInWorkflowWriteFileStoreHandler;
import org.knime.core.data.v2.RowKeyType;
import org.knime.core.data.v2.ValueSchema;
import org.knime.core.util.DuplicateChecker;
import org.knime.core.util.DuplicateKeyException;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class DuplicateCheckWritableTest extends ColumnarTest {

    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4);

    private static final ColumnarValueSchema SCHEMA;
    static {
        final DataTableSpec spec = createSpec();
        final ValueSchema valueSchema =
            ValueSchema.create(spec, RowKeyType.CUSTOM, NotInWorkflowWriteFileStoreHandler.create());
        SCHEMA = ColumnarValueSchemaUtils.create(valueSchema);
    }

    @AfterClass
    public static void tearDownTests() {
        EXECUTOR.shutdown();
    }

    private static DuplicateCheckWritable generateDuplicateCheckStore(final BatchWritable delegate) {
        return new DuplicateCheckWritable(delegate, new DuplicateChecker(), EXECUTOR);
    }

    @Test(expected = DuplicateKeyException.class)
    public void testWriteDuplicate() throws IOException {
        try (final BatchStore delegate = TestBatchStore.create(SCHEMA)) {
            final DuplicateCheckWritable store = generateDuplicateCheckStore(delegate);
            final WriteBatch[] batches = createBatches(store, 2, 2);
            writeRowKeys(batches, "1", "1");
            closeWriteReleaseBatches(store, batches);
        }
    }

    @Test
    public void testWriteRead() throws IOException {
        try (final BatchStore delegate = TestBatchStore.create(SCHEMA)) {
            final DuplicateCheckWritable store = generateDuplicateCheckStore(delegate);
            writeDefaultTable(store);
            readDefaultTable(delegate);
            readDefaultTable(delegate);
        }
    }

    @Test
    public void testWriterSingleton() throws IOException {
        try (final BatchStore delegate = TestBatchStore.create(SCHEMA)) {
            final DuplicateCheckWritable store = generateDuplicateCheckStore(delegate);
            try (final BatchWriter writer1 = store.getWriter(); final BatchWriter writer2 = store.getWriter()) {
                assertEquals(writer1, writer2);
            }
        }
    }

}
