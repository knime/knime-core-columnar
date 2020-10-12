///*
// * ------------------------------------------------------------------------
// *
// *  Copyright by KNIME AG, Zurich, Switzerland
// *  Website: http://www.knime.com; Email: contact@knime.com
// *
// *  This program is free software; you can redistribute it and/or modify
// *  it under the terms of the GNU General Public License, Version 3, as
// *  published by the Free Software Foundation.
// *
// *  This program is distributed in the hope that it will be useful, but
// *  WITHOUT ANY WARRANTY; without even the implied warranty of
// *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// *  GNU General Public License for more details.
// *
// *  You should have received a copy of the GNU General Public License
// *  along with this program; if not, see <http://www.gnu.org/licenses>.
// *
// *  Additional permission under GNU GPL version 3 section 7:
// *
// *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
// *  Hence, KNIME and ECLIPSE are both independent programs and are not
// *  derived from each other. Should, however, the interpretation of the
// *  GNU GPL Version 3 ("License") under any applicable laws result in
// *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
// *  you the additional permission to use and propagate KNIME together with
// *  ECLIPSE with only the license terms in place for ECLIPSE applying to
// *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
// *  license terms of ECLIPSE themselves allow for the respective use and
// *  propagation of ECLIPSE together with KNIME.
// *
// *  Additional permission relating to nodes for KNIME that extend the Node
// *  Extension (and in particular that are based on subclasses of NodeModel,
// *  NodeDialog, and NodeView) and that only interoperate with KNIME through
// *  standard APIs ("Nodes"):
// *  Nodes are deemed to be separate and independent programs and to not be
// *  covered works.  Notwithstanding anything to the contrary in the
// *  License, the License does not apply to Nodes, you are not required to
// *  license Nodes under the License, and you are granted a license to
// *  prepare and propagate Nodes, in each case even if such Nodes are
// *  propagated with or for interoperation with KNIME.  The owner of a Node
// *  may freely choose the license terms applicable to such Node, including
// *  when such Node is propagated with or for interoperation with KNIME.
// * ---------------------------------------------------------------------
// */
//
//package org.knime.core.columnar.domain;
//
//import java.util.LinkedHashSet;
//import java.util.Set;
//
//import org.knime.core.columnar.data.StringData.StringReadData;
//
///**
// * Domain for string values.
// *
// * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
// * @since 4.3
// */
//public final class StringDomain extends AbstractNominalDomain<String> {
//
//    /** Empty string domain **/
//    public static final StringDomain EMPTY = new StringDomain();
//
//    private StringDomain() {
//    }
//
//    /**
//     * @param values The set of nominal values described by this domain. Can be {@code null} in which case this domain
//     *            will be {@link #isValid() invalid}. An empty set will create a valid, empty domain (however, in this
//     *            case, using {@link #EMPTY} should be preferred instead to avoid object creation).
//     */
//    public StringDomain(final Set<String> values) {
//        super(values);
//    }
//
//    /**
//     * Calculates the domain of {@link StringReadData}
//     *
//     * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
//     */
//    public static final class StringDomainCalculator extends AbstractDomainCalculator<StringReadData, StringDomain> {
//
//        private final int m_numMaxValues;
//
//        /**
//         * Create calculator without initialization.
//         *
//         * @param numMaxValues max distinct string values
//         */
//        public StringDomainCalculator(final int numMaxValues) {
//            this(null, numMaxValues);
//        }
//
//        /**
//         * Create calculator with initialization.
//         *
//         * @param initialDomain the initial domain
//         * @param numMaxValues max distinct string valuesO
//         */
//        public StringDomainCalculator(final StringDomain initialDomain, final int numMaxValues) {
//            super(new StringDomainMerger(initialDomain, numMaxValues));
//            m_numMaxValues = numMaxValues;
//        }
//
//        @Override
//        public StringDomain calculateDomain(final StringReadData data) {
//            // TODO: more efficient implementation?
//            // Preserve order
//            Set<String> values = new LinkedHashSet<>();
//            for (int i = 0; i < data.length(); i++) {
//                if (!data.isMissing(i)) {
//                    values.add(data.getObject(i));
//                    if (values.size() > m_numMaxValues) {
//                        // Null indicates that domain could not be computed due to excessive
//                        // distinct elements. Computed domain will be marked invalid.
//                        values = null;
//                        break;
//                    }
//                }
//            }
//            return new StringDomain(values);
//        }
//    }
//
//    private static final class StringDomainMerger extends AbstractNominalDomainMerger<String, StringDomain> {
//
//        public StringDomainMerger(final StringDomain initialDomain, final int numMaxValues) {
//            super(initialDomain != null ? initialDomain : StringDomain.EMPTY, numMaxValues);
//        }
//
//        @Override
//        protected StringDomain createMergedDomain(final Set<String> mergedValues) {
//            return new StringDomain(mergedValues);
//        }
//    }
//
//}
