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
package org.knime.core.data.columnar.table.virtual;

import org.knime.core.data.IDataRepository;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Persistor for saving and loading objects to and from NodeSettings.
 * Implementations must
 * <ul>
 * <li>be public (even though they might be a static inner class of a package-private or even private class)
 * <li>provide a constructor with no arguments
 * </ul>
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @param <F> the type of ColumnarMapperFactory the persistor persists
 */
public interface Persistor<F> {

    /**
     * Saves the given factory into the settings object.
     *
     * @param factory to save
     * @param settings to save to
     */
    void save(final F factory, final NodeSettingsWO settings);

    /**
     * Loads a factory from the given settings and the provided context.
     *
     * @param settings to load from
     * @param context of the loading
     * @return the loaded factory
     * @throws InvalidSettingsException if the settings are invalid
     */
    F load(final NodeSettingsRO settings, final LoadContext context) throws InvalidSettingsException;

    /**
     * @return the {@link Class} of the persisted objects
     */
    Class<? extends F> getPersistedType();

    /**
     * Represents the context in which a MapperFactory is loaded.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    interface LoadContext {

    	/**
    	 * Provides access to the data repository of the current context (typically a workflow).
    	 *
    	 * @return the data repository of the current context
    	 */
        IDataRepository getDataRepository();
    }
}
