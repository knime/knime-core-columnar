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
 *   13 Oct 2020 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.columnar.cache.object.shared;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

import org.knime.core.columnar.cache.ColumnDataUniqueId;
import org.knime.core.columnar.cache.object.ObjectCache;
import org.knime.core.columnar.cache.object.ObjectReadCache;

/**
 * A cache for in-heap storing of object data that can be shared between multiple {@link ObjectCache ObjectCaches} and
 * {@link ObjectReadCache ObjectReadCaches}. Cached data is ephemeral, i.e, object data referenced only in the cache may
 * be reclaimed by the garbage collector at any point in time.
 *
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
public interface SharedObjectCache {

    /**
     * Either returns the value associated with {@link ColumnDataUniqueId key} or computes the value using the
     * mappingFunction and adds it to the cache.
     *
     * @param key identifying the cached object
     * @param mappingFunction to compute the cached object if it is absent
     * @return the cached object
     * @see Map#computeIfAbsent(Object, Function)
     */
    Object computeIfAbsent(ColumnDataUniqueId key, Function<ColumnDataUniqueId, Object> mappingFunction);

    /**
     * Puts the provided value into the cache.
     *
     * @param key referring to value
     * @param value the value to cache
     * @see Map#put(Object, Object)
     */
    void put(ColumnDataUniqueId key, Object value);

    /**
     * Removes the values associated with the provided keys from the cache.
     *
     * @param keys of the value to remove from the cache
     */
    void removeAll(final Collection<ColumnDataUniqueId> keys);

}
