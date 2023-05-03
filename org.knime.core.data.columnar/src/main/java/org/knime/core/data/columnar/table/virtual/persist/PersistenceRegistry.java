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
 *   Mar 14, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table.virtual.persist;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.IExtension;
import org.eclipse.core.runtime.IExtensionPoint;
import org.eclipse.core.runtime.Platform;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Provides persistors for ColumnarMapperFactories that are registered at the ColumnarMapperFactoryPersistence extension
 * point.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class PersistenceRegistry {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PersistenceRegistry.class);

    private static final String EXT_POINT_ID = "org.knime.core.data.columnar.Persistence";

    private final Map<String, UntypedPersistor> m_persistenceRegistry;

    // lazily initialized to prevent potential exceptions during class loading
    private static PersistenceRegistry instance;

    private PersistenceRegistry() {
        IExtensionPoint extensionPoint = Platform.getExtensionRegistry().getExtensionPoint(EXT_POINT_ID);
        m_persistenceRegistry = Stream.of(extensionPoint.getExtensions())//
            .map(IExtension::getConfigurationElements)//
            .flatMap(Stream::of)//
            .map(PersistenceRegistry::parseExtension)//
            .flatMap(Optional::stream)//
            .collect(Collectors.toMap(Persistence::persistableClass,
                Persistence::persistor));
    }

    private static Optional<Persistence>
        parseExtension(final IConfigurationElement configElement) {
        var factory = configElement.getAttribute("persistable");
        try {
            var persistor = (Persistor<?>)configElement.createExecutableExtension("persistor");
            return Optional.of(
                new Persistence(factory, new UntypedPersistor(persistor)));
        } catch (CoreException ex) {
            LOGGER.error("Failed to create persistor '%s' for ColumnarMapperFactory '%s'."
                .formatted(configElement.getAttribute("persistor"), factory), ex);
            return Optional.empty();
        }
    }

    private record Persistence(String persistableClass, UntypedPersistor persistor) {//NOSONAR
    }

    static Optional<UntypedPersistor> getPersistor(final Class<?> persistableClass) {
        return getPersistor(persistableClass.getName());
    }

    static Optional<UntypedPersistor> getPersistor(final String mapperFactoryClassName) {
        return Optional.ofNullable(getInstance().m_persistenceRegistry.get(mapperFactoryClassName));
    }

    private static synchronized PersistenceRegistry getInstance() {
        if (instance == null) {
            instance = new PersistenceRegistry();
        }
        return instance;
    }

    static final class UntypedPersistor implements Persistor<Object> {

        private final Persistor<?> m_typedPersistor;

        UntypedPersistor(final Persistor<?> typedPersistor) {
            m_typedPersistor = typedPersistor;
        }

        @Override
        public Object load(final NodeSettingsRO settings, final LoadContext ctx)
            throws InvalidSettingsException {
            return m_typedPersistor.load(settings, ctx);
        }

        @Override
        public void save(final Object factory, final NodeSettingsWO settings) {
            uncheckedSave(factory, m_typedPersistor, settings);
        }

        @SuppressWarnings("unchecked")
        private static <F> void uncheckedSave(final Object factory, final Persistor<F> persistor,
            final NodeSettingsWO settings) {
            persistor.save((F)factory, settings);
        }

    }
}
