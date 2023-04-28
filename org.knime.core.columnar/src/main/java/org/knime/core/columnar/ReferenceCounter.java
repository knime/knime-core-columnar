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
 *   Mar 22, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.columnar;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements {@link ReferencedData#retain()} and {@link ReferencedData#release()} and can be used by other classes for
 * convenience.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class ReferenceCounter {

    private final AtomicInteger m_counter;


    /**
     * Constructor for an instance that starts with a reference count of 1.
     */
    public ReferenceCounter() {
        m_counter = new AtomicInteger(1);
    }

    /**
     * Retains the object.
     *
     * @throws IllegalStateException if the object has already been released
     * @see ReferencedData#retain()
     */
    public void retain() {
        if (!tryRetain()) {
            throw new IllegalStateException("Attempted retain after ReferencedData has already been released.");
        }
    }

    /**
     * Tries to retain the object.
     *
     * @return true if the object has not already been released or false otherwise
     */
    public boolean tryRetain() {
        return m_counter.getAndUpdate(x -> x > 0 ? (x + 1) : x) > 0;
    }

    /**
     * Releases a reference of the object.
     * @return true if the reference count hits 0, otherwise false
     * @see ReferencedData#release()
     */
    public boolean release() {
        int count = m_counter.decrementAndGet();
        if (count == 0) {
            return true;
        } else if (count < 0) {
            throw new IllegalStateException("Already released data was released again");
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return m_counter.toString();
    }

}
