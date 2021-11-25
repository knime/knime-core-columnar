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
 *   Oct 27, 2021 (Carsten Haubold, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar.arrow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.knime.core.table.schema.DataSpecs.DICT_ENCODING;
import static org.knime.core.table.schema.DataSpecs.LIST;
import static org.knime.core.table.schema.DataSpecs.STRING;
import static org.knime.core.table.schema.DataSpecs.STRUCT;
import static org.knime.core.table.schema.DataSpecs.VARBINARY;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.data.ListData.ListReadData;
import org.knime.core.columnar.data.ListData.ListWriteData;
import org.knime.core.columnar.data.StringData.StringReadData;
import org.knime.core.columnar.data.StringData.StringWriteData;
import org.knime.core.columnar.data.StructData.StructReadData;
import org.knime.core.columnar.data.StructData.StructWriteData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryReadData;
import org.knime.core.columnar.data.VarBinaryData.VarBinaryWriteData;
import org.knime.core.columnar.data.dictencoding.DictElementCache;
import org.knime.core.columnar.data.dictencoding.DictEncodedBatchWriter;
import org.knime.core.columnar.data.dictencoding.DictEncodedRandomAccessBatchReader;
import org.knime.core.columnar.filter.DefaultColumnSelection;
import org.knime.core.columnar.store.BatchStore;
import org.knime.core.columnar.store.ColumnStoreFactory;
import org.knime.core.columnar.store.FileHandle;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectDeserializer;
import org.knime.core.table.schema.VarBinaryDataSpec.ObjectSerializer;

/**
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("javadoc")
public class DictEncodedBatchStoreTest {

    private FileHandle m_tempPath;

    @Before
    public void before() throws IOException {
        m_tempPath = ArrowTestUtils.createTmpKNIMEArrowFileSupplier();
    }

    @After
    public void after() throws IOException {
        m_tempPath.delete();
    }

    @Test
    public void testListOfDictEncodedString() throws IOException {
        var columnarSchema = ColumnarSchema.of(LIST.of(STRING(DICT_ENCODING)), STRING);
        var cache = new DictElementCache();
        final ColumnStoreFactory factory = new ArrowColumnStoreFactory();
        try (BatchStore batchStore = factory.createStore(columnarSchema, m_tempPath)) {
            try(BatchWriter baseWriter = batchStore.getWriter();
                DictEncodedBatchWriter wrappedWriter = new DictEncodedBatchWriter(baseWriter, columnarSchema, cache)
                ) {
                var wrappedBatch = wrappedWriter.create(5);

                final var data = (ListWriteData)wrappedBatch.get(0);
                assertNotEquals(ListWriteData.class, data.getClass()); // should be DecoratedListWriteData

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
            try(var wrappedReader = new DictEncodedRandomAccessBatchReader(batchStore, selection, columnarSchema, cache)
                ) {
                var batch = wrappedReader.readRetained(0);

                final var data = (ListReadData)batch.get(0);
                assertNotEquals(ListReadData.class, data.getClass()); // should be DecoratedListReadData

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

    @Test
    public void testListOfDictEncodedVarBinary() throws IOException {
        var columnarSchema = ColumnarSchema.of(LIST.of(VARBINARY(DICT_ENCODING)), STRING);
        var cache = new DictElementCache();
        final ColumnStoreFactory factory = new ArrowColumnStoreFactory();

        ObjectSerializer<String> serializer = (out, s) -> out.writeUTF(s);
        ObjectDeserializer<String> deserializer = in -> in.readUTF();

        try (BatchStore batchStore = factory.createStore(columnarSchema, m_tempPath)) {
            try(BatchWriter baseWriter = batchStore.getWriter();
                DictEncodedBatchWriter wrappedWriter = new DictEncodedBatchWriter(baseWriter, columnarSchema, cache)
                ) {
                var wrappedBatch = wrappedWriter.create(5);

                final var data = (ListWriteData)wrappedBatch.get(0);
                assertNotEquals(ListWriteData.class, data.getClass()); // should be DecoratedListWriteData

                final var slicedData0 = (VarBinaryWriteData)data.createWriteData(0, 2);
                slicedData0.setObject(0, "foo", serializer);
                slicedData0.setObject(1, "foo", serializer);
                final var slicedData1 = (VarBinaryWriteData)data.createWriteData(1, 3);
                slicedData1.setObject(0, "bar", serializer);
                slicedData1.setObject(1, "bar", serializer);
                slicedData1.setObject(2, "foo", serializer);
                final var finishedBatch = wrappedBatch.close(5);
                wrappedWriter.write(finishedBatch);
                finishedBatch.release();
            }

            final var selection = new DefaultColumnSelection(columnarSchema.numColumns());
            try(var wrappedReader = new DictEncodedRandomAccessBatchReader(batchStore, selection, columnarSchema, cache)
                ) {
                var batch = wrappedReader.readRetained(0);

                final var data = (ListReadData)batch.get(0);
                assertNotEquals(ListReadData.class, data.getClass()); // should be DecoratedListReadData

                final var slicedData0 = (VarBinaryReadData)data.createReadData(0);
                assertEquals("foo", slicedData0.getObject(0, deserializer));
                assertEquals("foo", slicedData0.getObject(1, deserializer));
                final var slicedData1 = (VarBinaryReadData)data.createReadData(1);
                assertEquals("bar", slicedData1.getObject(0, deserializer));
                assertEquals("bar", slicedData1.getObject(1, deserializer));
                assertEquals("foo", slicedData1.getObject(2, deserializer));
                batch.release();
            }
        }
    }

    @Test
    public void testStructOfDictEncodedString() throws IOException {
        var columnarSchema = ColumnarSchema.of(STRUCT.of(STRING(DICT_ENCODING), STRING));
        var cache = new DictElementCache();
        final ColumnStoreFactory factory = new ArrowColumnStoreFactory();
        try (BatchStore batchStore = factory.createStore(columnarSchema, m_tempPath)) {
            try(BatchWriter baseWriter = batchStore.getWriter();
                DictEncodedBatchWriter wrappedWriter = new DictEncodedBatchWriter(baseWriter, columnarSchema, cache)
                ) {
                var wrappedBatch = wrappedWriter.create(5);

                final var data = (StructWriteData)wrappedBatch.get(0);
                assertNotEquals(StructWriteData.class, data.getClass()); // should be DecoratedStructWriteData

                final var slicedData0 = (StringWriteData)data.getWriteDataAt(0);
                slicedData0.setString(0, "foo");
                slicedData0.setString(1, "foo");
                slicedData0.setString(2, "bar");
                slicedData0.setString(3, "bar");
                slicedData0.setString(4, "foo");
                final var slicedData1 = (StringWriteData)data.getWriteDataAt(1);
                slicedData1.setString(0, "bar");
                slicedData1.setString(1, "foo");
                slicedData1.setString(2, "bar");
                slicedData1.setString(3, "bar");
                slicedData1.setString(4, "foo");
                final var finishedBatch = wrappedBatch.close(5);
                wrappedWriter.write(finishedBatch);
                finishedBatch.release();
            }

            final var selection = new DefaultColumnSelection(columnarSchema.numColumns());
            try(var wrappedReader = new DictEncodedRandomAccessBatchReader(batchStore, selection, columnarSchema, cache)
                ) {
                var batch = wrappedReader.readRetained(0);

                final var data = (StructReadData)batch.get(0);
                assertNotEquals(StructReadData.class, data.getClass()); // should be DecoratedStructReadData

                final var slicedData0 = (StringReadData)data.getReadDataAt(0);
                assertEquals("foo", slicedData0.getString(0));
                assertEquals("foo", slicedData0.getString(1));
                assertEquals("bar", slicedData0.getString(2));
                assertEquals("bar", slicedData0.getString(3));
                assertEquals("foo", slicedData0.getString(4));
                final var slicedData1 = (StringReadData)data.getReadDataAt(1);
                assertEquals("bar", slicedData1.getString(0));
                assertEquals("foo", slicedData1.getString(1));
                assertEquals("bar", slicedData1.getString(2));
                assertEquals("bar", slicedData1.getString(3));
                assertEquals("foo", slicedData1.getString(4));
                batch.release();
            }
        }
    }
}
