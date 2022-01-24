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
 *   Jul 16, 2021 (Carsten Haubold, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.data.dictencoding;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.knime.core.table.schema.DataSpecs.DICT_ENCODING;
import static org.knime.core.table.schema.DataSpecs.LIST;
import static org.knime.core.table.schema.DataSpecs.STRING;

import java.io.IOException;
import java.util.Optional;

import org.junit.Test;
import org.knime.core.columnar.TestBatchStoreUtils;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.ListData.ListReadData;
import org.knime.core.columnar.data.ListData.ListWriteData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.columnar.data.dictencoding.DecoratedListData.DecoratedListReadData;
import org.knime.core.columnar.data.dictencoding.DecoratedListData.DecoratedListWriteData;
import org.knime.core.columnar.data.dictencoding.DecoratedStructData.DecoratedStructReadData;
import org.knime.core.columnar.data.dictencoding.DecoratedStructData.DecoratedStructWriteData;
import org.knime.core.columnar.data.dictencoding.DictDecodedStringData.DictDecodedStringWriteData;
import org.knime.core.columnar.data.dictencoding.DictDecodedVarBinaryData.DictDecodedVarBinaryWriteData;
import org.knime.core.columnar.filter.DefaultColumnSelection;
import org.knime.core.columnar.testing.ColumnarTest;
import org.knime.core.columnar.testing.DefaultTestBatchStore;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait;
import org.knime.core.table.schema.traits.DataTraitUtils;
import org.knime.core.table.schema.traits.DataTraits;

/**
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public class DictEncodedBatchStoreTest extends ColumnarTest {
    @Test
    public void testWrappedWriter() {
        var columnarSchema = TestBatchStoreUtils.createDefaultSchema();
        var cache = new DictElementCache();
        try (DefaultTestBatchStore batchStore = DefaultTestBatchStore.create(columnarSchema);
                BatchWriter baseWriter = batchStore.getWriter();
                DictEncodedBatchWriter wrappedWriter = new DictEncodedBatchWriter(baseWriter, columnarSchema, cache);
            ) {
            var baseBatch = baseWriter.create(5);
            var wrappedBatch = wrappedWriter.create(5);

            checkDictEncodedWriteData(columnarSchema, baseBatch, wrappedBatch);

            wrappedBatch.close(5).release();
            baseBatch.close(5).release();
        } catch (IOException ex1) {
            fail();
        }
    }

    @Test
    public void testListOfDictEncodedString() throws IOException {
        var columnarSchema = ColumnarSchema.of(LIST.of(STRING(DICT_ENCODING)), STRING);
        var cache = new DictElementCache();
        try (DefaultTestBatchStore batchStore = DefaultTestBatchStore.create(columnarSchema)) {
            try(BatchWriter baseWriter = batchStore.getWriter();
                DictEncodedBatchWriter wrappedWriter = new DictEncodedBatchWriter(baseWriter, columnarSchema, cache)
                ) {
                var wrappedBatch = wrappedWriter.create(5);

                final var data = (ListWriteData)wrappedBatch.get(0);
                assertEquals(DecoratedListWriteData.class, data.getClass());

                final var slicedData0 = (StringWriteData)data.createWriteData(0, 2);
                slicedData0.setString(0, "foo");
                slicedData0.setString(1, "foo");
                final var slicedData1 = (StringWriteData)data.createWriteData(1, 3);
                slicedData1.setString(0, "bar");
                slicedData1.setString(1, "bar");
                slicedData1.setString(2, "foo");
                final var finishedBatch = wrappedBatch.close(5);
                wrappedWriter.write(finishedBatch);
                finishedBatch.release();
            }

            final var selection = new DefaultColumnSelection(columnarSchema.numColumns());
            try(var wrappedReader = batchStore.createRandomAccessReader(selection)) {
                var batch = wrappedReader.readRetained(0);

                final var data = (ListReadData)batch.get(0);

                final var slicedData0 = (StringReadData)data.createReadData(0);
                assertEquals("foo", slicedData0.getString(0));
                assertEquals("foo", slicedData0.getString(1));
                final var slicedData1 = (StringReadData)data.createReadData(1);
                assertEquals("bar", slicedData1.getString(0));
                assertEquals("bar", slicedData1.getString(1));
                assertEquals("foo", slicedData1.getString(2));
                batch.release();
            }
        }
    }

    private static void checkDictEncodedWriteData(final ColumnarSchema columnarSchema, final WriteBatch baseBatch,
        final WriteBatch wrappedBatch) {
        for (int c = 0; c < columnarSchema.numColumns(); c++) {
            final var wrappedClass = wrappedBatch.get(c).getClass();
            var expectedClass =
                getExpectedDictEncodingClass(columnarSchema.getSpec(c), columnarSchema.getTraits(c), false);
            if (expectedClass.isPresent()) {
                assertEquals(expectedClass.get(), wrappedClass);
            } else {
                assertEquals(baseBatch.get(c).getClass(), wrappedClass);
            }
        }
    }

    private static Optional<Class<?>> getExpectedDictEncodingClass(final DataSpec spec, final DataTraits traits,
        final boolean isRead) {
        if (DataTraitUtils.containsDataTrait(DictEncodingTrait.class, traits)) {
            if (spec == StringDataSpec.INSTANCE) {
                return Optional.of(isRead ? null : DictDecodedStringWriteData.class);
            } else if (spec == VarBinaryDataSpec.INSTANCE) {
                return Optional.of(isRead ? null : DictDecodedVarBinaryWriteData.class);
            } else if (spec instanceof StructDataSpec) {
                return Optional.of(isRead ? DecoratedStructReadData.class : DecoratedStructWriteData.class);
            } else if (spec instanceof ListDataSpec) {
                return Optional.of(isRead ? DecoratedListReadData.class : DecoratedListWriteData.class);
            } else {
                throw new IllegalArgumentException("Dict Encoding for type " + spec.toString() + " not tested yet");
            }
        } else {
            return Optional.empty();
        }
    }

    @Test
    public void testWrappedBatchStore() throws IOException {
        var columnarSchema = TestBatchStoreUtils.createDefaultSchema();
        try (final var baseStore = DefaultTestBatchStore.create(columnarSchema);
             final var batchStore = new DictEncodedBatchWritableReadable(baseStore, baseStore)) {
            try (var writer = batchStore.getWriter()) {
                assertEquals(writer.getClass(), DictEncodedBatchWriter.class);
            }
        }
    }

    @Test
    public void testWrappedBatchReadStore() throws IOException {
        var columnarSchema = TestBatchStoreUtils.createDefaultSchema();
        try (final var baseStore = DefaultTestBatchStore.create(columnarSchema);
                final var batchStore = new DictEncodedBatchWritableReadable(baseStore, baseStore)) {
            try (var writer = batchStore.getWriter()) {
                assertEquals(writer.getClass(), DictEncodedBatchWriter.class);
            }
        }
    }
}
