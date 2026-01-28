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
package org.knime.core.columnar.arrow.offheap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.knime.core.columnar.arrow.compress.ArrowCompression;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.store.FileHandle;

/**
 * The ArrowColumnDataWriter writes batches of columns to an Arrow file.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
class OffHeapArrowBatchWriter extends OffHeapArrowSimpleBatchWriter {

    private boolean m_firstWrite;
    private boolean m_usesDictionaries;
    private final OffHeapArrowColumnDataFactory[] m_factories;

    OffHeapArrowBatchWriter(final FileHandle file, final OffHeapArrowColumnDataFactory[] factories,
        final ArrowCompression compression, final BufferAllocator allocator) {

        super(file, factories, compression, allocator);
        this.m_factories = factories;
        this.m_firstWrite = true;
    }

    @Override
    public synchronized void write(final ReadBatch batch) throws IOException {

        if (m_firstWrite) {
            m_usesDictionaries = usesDictionaries(m_factories, batch);
            System.out.println("+++ useDictionaries = " + m_usesDictionaries);
            m_firstWrite = false;
        }

        super.write(batch);
    }

    @Override
    protected Schema createSchema(final ReadBatch batch, final Map<String, String> metadata) {

        if (!m_usesDictionaries) {
            return super.createSchema(batch, metadata);
        }

        final List<Field> fields = new ArrayList<>(m_factories.length);
        final AtomicInteger mappedId = new AtomicInteger(0);

        // Loop and collect fields with mapped dictionary IDs
        for (int i = 0; i < m_factories.length; i++) {
            final NullableReadData data = batch.get(i);
            final OffHeapArrowColumnDataFactory factory = m_factories[i];
            @SuppressWarnings("resource") // Vector resource is handled by the ColumnData
            final FieldVector vector = factory.getVector(data);
            final DictionaryProvider dictionaries = factory.getDictionaries(data);
            final Field field = vector.getField();
            fields.add(mapField(field, dictionaries, mappedId::getAndIncrement));
        }

        return new Schema(fields, metadata);
    }

    /** Map the dictionary ids in the given field to new unique ids and convert the type to the message format type */
    private static Field mapField(final Field field, final DictionaryProvider dictionaries,
        final LongSupplier nextMappedId) {

        final DictionaryEncoding encoding = field.getDictionary();
        final DictionaryEncoding mappedEncoding;
        final ArrowType mappedType;
        final List<Field> children;
        if (encoding == null) {
            // No dictionary encoding: Nothing to do
            mappedEncoding = null;
            mappedType = field.getType();
            children = field.getChildren();
        } else {
            // Map the id of this dictionary encoding
            final long id = encoding.getId();
            final long mappedId = nextMappedId.getAsLong();
            final Dictionary dictionary = dictionaries.lookup(id);
            @SuppressWarnings("resource") // Vector resource is handled by the ColumnData
            final FieldVector vector = dictionary.getVector();

            // Create a mapped DictionaryEncoding with the new id
            mappedEncoding = new DictionaryEncoding(mappedId, encoding.isOrdered(), encoding.getIndexType());
            mappedType = dictionary.getVectorType();
            // The children of this field are the children of the dictionary field
            children = vector.getField().getChildren();
        }

        // Call recursively for the children
        final List<Field> mappedChildren = new ArrayList<>(field.getChildren().size());
        for (final Field child : children) {
            mappedChildren.add(mapField(child, dictionaries, nextMappedId));
        }

        // Create the Field
        final FieldType fieldType = new FieldType(field.isNullable(), mappedType, mappedEncoding, field.getMetadata());
        return new Field(field.getName(), fieldType, mappedChildren);
    }

    // TODO (TP): Where to put this? It should be possible to figure this out from factories alone.
    static boolean usesDictionaries(final OffHeapArrowColumnDataFactory[] factories, final ReadBatch batch) {
        for (int i = 0; i < factories.length; i++) {
            final NullableReadData data = batch.get(i);
            final OffHeapArrowColumnDataFactory factory = factories[i];
            final DictionaryProvider dictionaries = factory.getDictionaries(data);
            if (dictionaries != null) {
                return true;
            }
        }
        return false;
    }

}
