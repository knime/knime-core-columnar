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
 *   Mar 10, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table.virtual.persist;

import static java.util.stream.Collectors.toUnmodifiableMap;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.IDataRepository;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.virtual.ColumnarVirtualTable.ColumnarMapperFactory;
import org.knime.core.data.columnar.table.virtual.ColumnarVirtualTable.ColumnarMapperWithRowIndexFactory;
import org.knime.core.data.columnar.table.virtual.ColumnarVirtualTable.WrappedColumnarMapperWithRowIndexFactory;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.ValueFactoryUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.spec.AppendMissingValuesTransformSpec;
import org.knime.core.table.virtual.spec.AppendTransformSpec;
import org.knime.core.table.virtual.spec.ConcatenateTransformSpec;
import org.knime.core.table.virtual.spec.IdentityTransformSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.RowIndexTransformSpec;
import org.knime.core.table.virtual.spec.SelectColumnsTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTableProperties;
import org.knime.core.table.virtual.spec.SourceTableProperties.CursorType;
import org.knime.core.table.virtual.spec.SourceTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;

import com.google.common.collect.Lists;

/**
 * Persistor for {@link TableTransform} objects.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class TableTransformNodeSettingsPersistor {

    private static final Map<Class<? extends TableTransformSpec>, TransformSpecPersistor> SPEC_TO_PERSISTOR =
        Stream.of(TransformSpecPersistor.values())
            .collect(toUnmodifiableMap(TransformSpecPersistor::specClass, Function.identity()));

    /**
     * @param tableTransform to save
     * @param settings to save to
     */
    public static void save(final TableTransform tableTransform, final NodeSettingsWO settings) {
        var traceBack = new TableTransformTraceBack(tableTransform);
        final Deque<TableTransform> transformsToTraverse = new ArrayDeque<>();
        final Map<TableTransform, Integer> transformIds = new IdentityHashMap<>();

        final NodeSettingsWO transformSettings = settings.addNodeSettings("transforms");
        final NodeSettingsWO connectionSettings = settings.addNodeSettings("connections");
        int connectionCount = 0;//NOSONAR

        traceBack.getSources().forEach(transformsToTraverse::push);

        while (!transformsToTraverse.isEmpty()) {
            final TableTransform transform = transformsToTraverse.pop();


            if (transformIds.containsKey(transform)) {
                continue;
            }

            final List<TableTransform> parentTransforms = transform.getPrecedingTransforms();
            if (parentTransforms.stream().anyMatch(Predicate.not(transformIds::containsKey))) {
                // We cannot process the transform yet because not all parents have been visited; guarantees
                // topological ordering.
                transformsToTraverse.addLast(transform);
                continue;
            }

            // Push children in reverse order. This is not necessary to guarantee the correctness of the
            // serialization logic, but it keeps the serialized format more intuitive (because transforms will
            // appear in the order in which they were defined programmatically).
            Lists.reverse(traceBack.getChildren(transform)).forEach(transformsToTraverse::push);

            int id = transformIds.size();
            saveTransformSpec(transform.getSpec(), transformSettings.addNodeSettings(Integer.toString(id)));
            transformIds.put(transform, id);

            for (int i = 0; i < parentTransforms.size(); i++) {//NOSONAR
                final TableTransform parentTransform = parentTransforms.get(i);
                final var connection = connectionSettings.addNodeSettings(Integer.toString(connectionCount));
                connectionCount++;
                saveConnection(transformIds.get(parentTransform), id, i, connection);
            }
        }
    }

    /**
     * Context for loading a TableTransform from NodeSettingsRO.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    public interface LoadContext {

        /**
         * @return the data repository to use for reading filestores
         */
        IDataRepository getDataRepository();
    }

    /**
     * @param settings to load from
     * @param ctx the context of the table that is loaded
     * @return the loaded transform
     * @throws InvalidSettingsException if the settings are invalid
     */
    public static TableTransform load(final NodeSettingsRO settings, final LoadContext ctx)
        throws InvalidSettingsException {

        var transformSettings = settings.getNodeSettings("transforms");
        var connectionSettings = settings.getNodeSettings("connections");

        var transformSpecs = new ArrayList<TableTransformSpec>();
        for (var key : transformSettings) {
            transformSpecs.add(loadSpec(transformSettings.getNodeSettings(key), ctx));
        }

        final Set<Integer> leafTransforms =
            new LinkedHashSet<>(IntStream.range(0, transformSpecs.size()).boxed().collect(Collectors.toList()));
        final Map<Integer, Map<Integer, Integer>> parentTransforms = new HashMap<>();
        for (final var key : connectionSettings) {
            var connection = connectionSettings.getNodeSettings(key);
            final int fromTransform = connection.getNodeSettings("from").getInt("transform");//NOSONAR
            leafTransforms.remove(fromTransform);
            final NodeSettingsRO to = connection.getNodeSettings("to");
            final int toTransform = to.getInt("transform");//NOSONAR
            final int toPort = to.getInt("port");//NOSONAR
            parentTransforms.computeIfAbsent(toTransform, k -> new HashMap<>()).put(toPort, fromTransform);
        }

        final Map<Integer, TableTransform> transforms = new HashMap<>();
        for (int i = 0; i < transformSpecs.size(); i++) {//NOSONAR
            resolveTransformsTree(i, transformSpecs, parentTransforms, transforms);
        }

        return transforms.get(leafTransforms.iterator().next());

    }

    private static TableTransformSpec loadSpec(final NodeSettingsRO specSettings, final LoadContext ctx)
        throws InvalidSettingsException {
        var type = specSettings.getString("type");
        var persistor = TransformSpecPersistor.valueOf(type);
        return persistor.load(specSettings.getNodeSettings("internal"), ctx);
    }

    private static void resolveTransformsTree(final int specIndex, final List<TableTransformSpec> transformSpecs,
        final Map<Integer, Map<Integer, Integer>> parentTransforms, final Map<Integer, TableTransform> transforms) {
        if (transforms.containsKey(specIndex)) {
            return;
        }
        final Map<Integer, Integer> parents = parentTransforms.get(specIndex);
        final List<TableTransform> resolvedParents;
        if (parents != null) {
            resolvedParents = new ArrayList<>(parents.size());
            for (int j = 0; j < parents.size(); j++) {//NOSONAR
                final int parentSpecIndex = parents.get(j);
                resolveTransformsTree(parentSpecIndex, transformSpecs, parentTransforms, transforms);
                resolvedParents.add(transforms.get(parentSpecIndex));
            }
        } else {
            resolvedParents = Collections.emptyList();
        }
        transforms.put(specIndex, new TableTransform(resolvedParents, transformSpecs.get(specIndex)));
    }

    static <T extends TableTransformSpec> void saveTransformSpec(final T transformSpec, final NodeSettingsWO settings) {
        var persistor = SPEC_TO_PERSISTOR.get(transformSpec.getClass());
        if (persistor == null) {
            throw new IllegalArgumentException(
                "Persisting specs of type '%s' is not supported.".formatted(transformSpec.getClass()));
        }
        settings.addString("type", persistor.name());
        persistor.save(transformSpec, settings.addNodeSettings("internal"));
    }

    private static void saveConnection(final int from, final int to, final int toPort, final NodeSettingsWO settings) {
        // Forward compatibility: make "from" an object to allow adding port information in a future
        // iteration.
        settings.addNodeSettings("from").addInt("transform", from);
        var toSettings = settings.addNodeSettings("to");
        toSettings.addInt("transform", to);
        toSettings.addInt("port", toPort);
    }

    @FunctionalInterface
    interface TransformSpecSaver<T extends TableTransformSpec> {//NOSONAR
        void save(T transformSpec, NodeSettingsWO settings);
    }

    @FunctionalInterface
    interface TransformSpecLoader<T extends TableTransformSpec> {
        T load(NodeSettingsRO settings, LoadContext ctx) throws InvalidSettingsException;
    }

    private static <T extends TableTransformSpec> TransformSpecSaver<T> noop() {
        return (s, t) -> {
        };
    }

    enum TransformSpecPersistor {
            SOURCE(//
                SourceTransformSpec.class, //
                (s, c) -> new SourceTransformSpec(UUID.fromString(s.getString("identifier")), new SourceTableProperties(null, CursorType.BASIC, -1)),
                (t, s) -> s.addString("identifier", t.getSourceIdentifier().toString())),
            APPEND(//
                AppendTransformSpec.class, //
                (s, c) -> new AppendTransformSpec(), //
                noop()),
            SELECT(//
                SelectColumnsTransformSpec.class, //
                (s, c) -> new SelectColumnsTransformSpec(s.getIntArray("included_columns")),
                (t, s) -> s.addIntArray("included_columns", t.getColumnSelection())),
            CONCATENATE(//
                ConcatenateTransformSpec.class, //
                (s, c) -> new ConcatenateTransformSpec(), //
                noop()),
            SLICE(//
                SliceTransformSpec.class, //
                (s, c) -> new SliceTransformSpec(s.getLong("from"), s.getLong("to")), //
                (t, s) -> {
                    s.addLong("from", t.getRowRangeSelection().fromIndex());
                    s.addLong("to", t.getRowRangeSelection().toIndex());
                }),
            IDENTITY(//
                IdentityTransformSpec.class, //
                (s, c) -> IdentityTransformSpec.INSTANCE, //
                noop()),
            ROWINDEX(//
                RowIndexTransformSpec.class, //
                (s, c) -> new RowIndexTransformSpec(s.getLong("offset")), //
                (t, s) -> s.addLong("offset", t.getOffset())),
            MAP(//
                MapTransformSpec.class, //
                (s, c) -> {
                    var mapperFactoryClass = s.getString("mapper_factory_class");
                    var persistor = PersistenceRegistry.getPersistor(mapperFactoryClass)
                        .orElseThrow(() -> new InvalidSettingsException(
                            "No persistor available for mapper factory %s.".formatted(mapperFactoryClass)));
                    final Object factory =
                        persistor.load(s.getNodeSettings("mapper_factory_settings"), c::getDataRepository);
                    final ColumnarMapperFactory mapperFactory;
                    if (factory instanceof ColumnarMapperWithRowIndexFactory f) {
                        mapperFactory = new WrappedColumnarMapperWithRowIndexFactory(f);
                    } else { // if (factory instanceof ColumnarMapperFactory) {
                        mapperFactory = (ColumnarMapperFactory)factory;
                    }
                    return new MapTransformSpec(s.getIntArray("column_indices"), mapperFactory);
                },
                (t, s) -> {//NOSONAR
                    s.addIntArray("column_indices", t.getColumnSelection());
                    final Object mapperFactory;
                    if (t.getMapperFactory() instanceof WrappedColumnarMapperWithRowIndexFactory wrapper) {
                        mapperFactory = wrapper.getMapperWithRowIndexFactory();
                        // mapperFactory is a ColumnarMapperWithRowIndexFactory
                    } else {
                        mapperFactory = t.getMapperFactory();
                        // mapperFactory is a ColumnarMapperFactory
                    }
                    var factoryClass = mapperFactory.getClass();
                    var persistor =
                            PersistenceRegistry.getPersistor(factoryClass)
                            .orElseThrow(() -> new IllegalArgumentException(
                                "No persistor for %s registered.".formatted(factoryClass)));
                    s.addString("mapper_factory_class", factoryClass.getName());
                    persistor.save(mapperFactory, s.addNodeSettings("mapper_factory_settings"));
                }
                ),
            APPEND_MISSING(//
                AppendMissingValuesTransformSpec.class, //
                (s, c) -> {
                    return new AppendMissingValuesTransformSpec(loadColumnarValueSchema(s, c.getDataRepository()));
                }, (t, s) -> {
                    var schema = t.getAppendedSchema();
                    if (schema instanceof ColumnarValueSchema valueSchema) {
                        saveMissingColumnsSchema(valueSchema, s);
                    } else {
                        throw new UnsupportedOperationException(
                            "Append missing transforms can only be persisted if they use a ColumnarValueSchema");
                    }
                });

        private final TransformSpecSaver<?> m_saver;

        private final TransformSpecLoader<?> m_loader;

        private final Class<? extends TableTransformSpec> m_specClass;

        private <T extends TableTransformSpec> TransformSpecPersistor(final Class<T> specClass,
            final TransformSpecLoader<T> loader, final TransformSpecSaver<T> saver) {
            m_saver = saver;
            m_loader = loader;
            m_specClass = specClass;
        }

        void save(final TableTransformSpec transformSpec, final NodeSettingsWO settings) {
            save(transformSpec, m_saver, settings);
        }

        @SuppressWarnings("unchecked")
        private static <T extends TableTransformSpec> void save(final TableTransformSpec transformSpec,
            final TransformSpecSaver<T> saver, final NodeSettingsWO settings) {
            saver.save((T)transformSpec, settings);
        }

        TableTransformSpec load(final NodeSettingsRO settings, final LoadContext ctx) throws InvalidSettingsException {
            return m_loader.load(settings, ctx);
        }

        Class<? extends TableTransformSpec> specClass() {
            return m_specClass;
        }

    }

    private static void saveMissingColumnsSchema(final ColumnarValueSchema schema, final NodeSettingsWO settings) {
        CheckUtils.checkArgument(!ColumnarValueSchemaUtils.hasRowID(schema),
            "A schema used for appending missing values must not have a RowID column because RowIDs can't be missing.");
        saveColumnarValueSchema(schema, settings);
    }

    private static void saveColumnarValueSchema(final ColumnarValueSchema schema, final NodeSettingsWO settings) {
        schema.getSourceSpec().save(settings.addNodeSettings("data_table_spec"));
        var valueFactorySettings = settings.addNodeSettings("value_factories");
        for (int i = 0; i < schema.numColumns(); i++) {
            ValueFactoryUtils.saveValueFactory(schema.getValueFactory(i),
                valueFactorySettings.addNodeSettings(Integer.toString(i)));
        }
    }

    private static ColumnarValueSchema loadColumnarValueSchema(final NodeSettingsRO settings,
        final IDataRepository dataRepository) throws InvalidSettingsException {
        var tableSpec = DataTableSpec.load(settings.getNodeSettings("data_table_spec"));
        // the schema contains no RowID
        var valueFactories = new ValueFactory<?, ?>[tableSpec.getNumColumns()];
        var valueFactorySettings = settings.getNodeSettings("value_factories");
        for (int i = 0; i < valueFactories.length; i++) {//NOSONAR
            valueFactories[i] = ValueFactoryUtils
                .loadValueFactory(valueFactorySettings.getNodeSettings(Integer.toString(i)), dataRepository);
        }
        return ColumnarValueSchemaUtils.create(tableSpec, valueFactories);
    }

}
