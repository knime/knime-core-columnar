/*
 * ------------------------------------------------------------------------
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
 * -------------------------------------------------------------------
 *
 * History
 *   Mar 26, 2020 (dietzc): created
 */
package org.knime.core.data.columnar.factory;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.IExtensionPoint;
import org.eclipse.core.runtime.IExtensionRegistry;
import org.eclipse.core.runtime.Platform;
import org.knime.core.columnar.store.ColumnStoreFactory;

import com.google.common.base.Preconditions;

/**
 * A registry collecting the column store factory extension point, whereby it expects exactly one contribution.
 *
 * @author Bernd Wiswedel, KNIME GmbH, Konstanz
 * @since 4.3
 *
 * @noreference This class is not intended to be referenced by clients.
 */
public final class ColumnStoreFactoryRegistry {

    private static final String EXT_POINT_ID = "org.knime.core.data.columnar.ColumnStoreFactory";

    private static ColumnStoreFactoryRegistry instance;

    private static ColumnStoreFactoryRegistry createInstance() {
        IExtensionRegistry registry = Platform.getExtensionRegistry();
        IExtensionPoint point = registry.getExtensionPoint(EXT_POINT_ID);

        IConfigurationElement[] configurationElements = Stream.of(point.getExtensions()) //
                .flatMap(ext -> Stream.of(ext.getConfigurationElements())) //
                .toArray(IConfigurationElement[]::new);
        Exception exception = null;
        ColumnStoreFactory factory = null;
        try {
            if (configurationElements.length < 1) {
                throw new IllegalStateException(String.format("No registrations to extension point \"%s\" "
                        + "-- unable to provide column store backend", EXT_POINT_ID));
            }
            if (configurationElements.length > 1) {
                String contributors = Arrays.stream(configurationElements) //
                        .map(IConfigurationElement::getDeclaringExtension) //
                        .map(ce -> "\"" + ce.getContributor().getName() + "\"") //
                        .collect(Collectors.joining(", "));
                throw new IllegalStateException(String.format("Multiple registrations to extension point \"%s\" (from "
                        + "%s) -- make sure to have exactly one installed. Won't use any", EXT_POINT_ID, contributors));
            }
            factory = (ColumnStoreFactory)configurationElements[0].createExecutableExtension("factory");
            Preconditions.checkState(factory != null, "Contribution to extension point \"%s\", contributed by \"%s\", "
                    + "must not be null", configurationElements[0].getContributor().getName());
        } catch (CoreException | IllegalStateException ex) {
            exception = ex;
        }
        return new ColumnStoreFactoryRegistry(factory, exception);
    }

    /** @return the instance to use. */
    public static ColumnStoreFactoryRegistry getOrCreateInstance() {
        synchronized (ColumnStoreFactoryRegistry.class) {
            if (instance == null) {
                instance = createInstance();
            }
            return instance;
        }
    }

    private final ColumnStoreFactory m_factorySingleton;
    private final Exception m_exceptionAtCreationTimeOrNull;

    private ColumnStoreFactoryRegistry(final ColumnStoreFactory factory, final Exception exception) {
        m_factorySingleton = factory;
        m_exceptionAtCreationTimeOrNull = exception;
        assert (m_factorySingleton == null) != (m_exceptionAtCreationTimeOrNull == null) :
            "one of the arguments must not be null";
    }

    /**
     * @return the factorySingleton the instance
     * @throws Exception thrown when instantiation initially failed
     */
    public ColumnStoreFactory getFactorySingleton() throws Exception { // NOSONAR
        if (m_factorySingleton != null) {
            return m_factorySingleton;
        }
        throw m_exceptionAtCreationTimeOrNull;
    }

    @Override
    public String toString() {
        if (m_exceptionAtCreationTimeOrNull != null) {
            return String.format("Empty registry due to %s: %s",
                m_exceptionAtCreationTimeOrNull.getClass().getSimpleName(),
                m_exceptionAtCreationTimeOrNull.getMessage());
        }
        return "Registry using instance " + m_factorySingleton.getClass().getName();
    }

}
