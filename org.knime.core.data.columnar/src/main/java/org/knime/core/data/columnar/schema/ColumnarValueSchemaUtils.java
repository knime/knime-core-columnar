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
package org.knime.core.data.columnar.schema;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.columnar.table.virtual.persist.Persistor.LoadContext;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.data.meta.DataColumnMetaData;
import org.knime.core.data.v2.RowKeyType;
import org.knime.core.data.v2.RowKeyValueFactory;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.ValueFactoryUtils;
import org.knime.core.data.v2.schema.ValueSchema;
import org.knime.core.data.v2.schema.ValueSchemaLoadContext;
import org.knime.core.data.v2.schema.ValueSchemaUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.schema.ColumnarSchema;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.traits.DataTraits;

/**
 * Utility class to work with {@link ColumnarValueSchema}s.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany.
 */
public final class ColumnarValueSchemaUtils {

    private ColumnarValueSchemaUtils() {
    }

    /**
     * Indicates that this schema does not store the cell serializer identifiers with the data, but separately. This was
     * the case prior to KNIME Analytics Platform 4.5.0.
     *
     * @param schema to check
     * @return true if schema was stores the cell serializers separately, meaning it was created before KNIME Analytics
     *         Platform 4.5.0
     */
    public static boolean storesDataCellSerializersSeparately(final ColumnarValueSchema schema) {
        return ValueSchemaUtils.storesDataCellSerializersSeparately(getValueSchema(schema));
    }

    /**
     * @return an empty {@link ColumnarValueSchema}
     */
    public static ColumnarValueSchema empty() {
        return EmptyColumnarValueSchema.INSTANCE;
    }

    /**
     * Loads a {@link ColumnarValueSchema} by combining the provided {@link ColumnarSchema} with the information stored
     * in the {@link LoadContext}.
     *
     * @param schema of the underlying store
     * @param context contains the DataTableSpec as well as the DataRepository and potential additional settings
     * @return the loaded {@link ColumnarValueSchema}
     * @throws InvalidSettingsException if the settings in the LoadContext were invalid
     */
    public static ColumnarValueSchema load(final ColumnarSchema schema, final ValueSchemaLoadContext context)
        throws InvalidSettingsException {
        return create(ValueSchemaUtils.load(schema, context));
    }

    private static ValueSchema getValueSchema(final ColumnarValueSchema schema) {
        if (schema instanceof DefaultColumnarValueSchema defaultSchema) {
            return defaultSchema.getSource();
        } else if (schema instanceof UpdatedColumnarValueSchema updatedSchema) {
            return getValueSchema(updatedSchema.getDelegate());
        } else {
            throw new IllegalArgumentException("Unsupported ColumnarValueSchema type: " + schema.getClass());
        }

    }

    /**
     * Create a new {@link ColumnarValueSchema} based on the provided {@link ValueSchema}.
     *
     * @param source the underlying {@link ValueSchema}.
     *
     * @return a new {@link ColumnarValueSchema}.
     */
    public static ColumnarValueSchema create(final ValueSchema source) {
        return new DefaultColumnarValueSchema(source);
    }

    /**
     * Create a new {@link ColumnarValueSchema} based on a {@link DataTableSpec} and {@link ValueFactory
     * ValueFactories}.
     *
     * @param spec of the table
     * @param valueFactories for the columns including the RowID
     * @return a new {@link ColumnarValueSchema}
     */
    public static final ColumnarValueSchema create(final DataTableSpec spec,
        final ValueFactory<?, ?>[] valueFactories) {
        return create(ValueSchemaUtils.create(spec, valueFactories));
    }

    /**
     * Creates a ColumnarValueSchema for the provided parameters.
     *
     * @param spec to create the value schema for
     * @param rowIDType the type of RowID to use
     * @param fsHandler FileStoreHandler used by some ValueFactories
     * @return a new schema corresponding to spec
     */
    public static ColumnarValueSchema create(final DataTableSpec spec, final RowKeyType rowIDType,
        final IWriteFileStoreHandler fsHandler) {
        final var rowKeyFactory = ValueFactoryUtils.getRowKeyValueFactory(rowIDType);
        final var columnValueFactories = spec.stream() //
            .map(DataColumnSpec::getType) //
            .map(t -> ValueFactoryUtils.getValueFactory(t, fsHandler));
        return create(spec, Stream.concat(Stream.of(rowKeyFactory), columnValueFactories) //
            .toArray(ValueFactory<?, ?>[]::new));
    }

    /**
     * Checks if a schema includes a RowID column.
     *
     * @param schema to check
     * @return true if the schema has a RowID column
     */
    public static boolean hasRowID(final ColumnarValueSchema schema) {
        return schema.numColumns() > 0 && schema.getValueFactory(0) instanceof RowKeyValueFactory;
    }

    /**
     * Updates the {@link DataTableSpec} of the passed source scheme with a new {@link DataTableSpec}, including the
     * domains provided in the {@link Map}.
     *
     * @param source the source {@link DataTableSpec}
     * @param domainMap the domains used for update.
     * @param metadataMap the columnar metadata used to update
     *
     * @return the updated {@link ColumnarValueSchema}
     */
    public static ColumnarValueSchema updateSource(final ColumnarValueSchema source,
        final Map<Integer, DataColumnDomain> domainMap, final Map<Integer, DataColumnMetaData[]> metadataMap) {
        final var result = new DataColumnSpec[source.numColumns() - 1];
        for (int i = 0; i < result.length; i++) {//NOSONAR
            final DataColumnSpec colSpec = source.getSourceSpec().getColumnSpec(i);
            final DataColumnDomain domain = domainMap.get(i + 1);
            final DataColumnMetaData[] metadata = metadataMap.get(i + 1);

            if (domain == null && metadata == null) {
                result[i] = colSpec;
            } else {
                final var creator = new DataColumnSpecCreator(colSpec);
                if (domain != null) {
                    creator.setDomain(domain);
                }

                for (final DataColumnMetaData element : metadata) {
                    creator.addMetaData(element, true);
                }

                result[i] = creator.createSpec();
            }
        }
        final var sourceName = source.getSourceSpec().getName();
        return new UpdatedColumnarValueSchema(new DataTableSpec(sourceName, result), source);
    }

    /**
     * Changes the DataTableSpec in the schema.
     *
     * @param schema to updated
     * @param spec to update the schema with (may have e.g. a different domains or different column names)
     * @return the schema with the updated spec
     * @throws IllegalArgumentException if the types in schema and spec don't match
     */
    public static ColumnarValueSchema updateDataTableSpec(final ColumnarValueSchema schema, final DataTableSpec spec) {
        return new UpdatedColumnarValueSchema(spec, schema);
    }

    /**
     * Assign new random column names.
     *
     * @param schema input schema
     * @return a new {@code ColumnarValueSchema}, equivalent to input {@code schema} but with new random column names.
     */
    public static ColumnarValueSchema renameToRandomColumnNames(final ColumnarValueSchema schema) {
        var valueFactories = new ValueFactory<?, ?>[schema.numColumns()];
        Arrays.setAll(valueFactories, schema::getValueFactory);

        final DataTableSpec sourceSpec = schema.getSourceSpec();
        var colSpecs = new DataColumnSpec[sourceSpec.getNumColumns()];
        Arrays.setAll(colSpecs, i -> {
            DataColumnSpecCreator creator = new DataColumnSpecCreator(sourceSpec.getColumnSpec(i));
            creator.setName("random-" + UUID.randomUUID().toString());
            return creator.createSpec();
        });
        var spec = new DataTableSpec(colSpecs);

        return create(spec, valueFactories);
    }

    private enum EmptyColumnarValueSchema implements ColumnarValueSchema {

            INSTANCE;

        private static final DataTableSpec EMPTY = new DataTableSpec();

        @Override
        public int numColumns() {
            return 0;
        }

        @Override
        public DataSpec getSpec(final int index) {
            throw emptyException();
        }

        @Override
        public Stream<DataSpec> specStream() {
            return Stream.empty();
        }

        @Override
        public DataTraits getTraits(final int index) {
            throw emptyException();
        }

        @Override
        public Iterator<DataSpec> iterator() {
            return Collections.emptyIterator();
        }

        @Override
        public DataTableSpec getSourceSpec() {
            return EMPTY;
        }

        @Override
        public <R extends ReadAccess, W extends WriteAccess> ValueFactory<R, W> getValueFactory(final int index) {
            throw emptyException();
        }

        private IndexOutOfBoundsException emptyException() {
            return new IndexOutOfBoundsException("This ColumnarValueSchema is empty.");
        }

        @Override
        public void save(final NodeSettingsWO settings) {
            // nothing to save
        }

    }

    public static ColumnarValueSchema selectColumns(final ColumnarValueSchema schema, final int... columnIndices) {

        var valueFactories = new ValueFactory<?, ?>[columnIndices.length];
        Arrays.setAll(valueFactories, i -> schema.getValueFactory(columnIndices[i]));

        final DataTableSpec sourceSpec = schema.getSourceSpec();
        var colSpecs = new DataColumnSpec[columnIndices.length];
        Arrays.setAll(colSpecs, i -> sourceSpec.getColumnSpec(columnIndices[i] - 1));
        var spec = new DataTableSpec(colSpecs);

        return create(spec, valueFactories);
    }

}
