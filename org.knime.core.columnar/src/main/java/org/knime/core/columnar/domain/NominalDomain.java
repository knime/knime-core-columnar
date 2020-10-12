
package org.knime.core.columnar.domain;

import java.util.Set;

/**
 * Interface to define {@link NominalDomain}s
 *
 * @param <T> type
 *
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
public interface NominalDomain<T> extends Domain {

    /**
     * Values defined by this domain.
     *
     * @return the values of the domain.
     */
    Set<T> getValues();
}
