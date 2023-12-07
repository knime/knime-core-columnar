# KNIME® Core Columnar

[![Jenkins](https://jenkins.knime.com/buildStatus/icon?job=knime-core-columnar%2Fmaster)](https://jenkins.knime.com/job/knime-core-columnar/job/master/)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=KNIME_knime-core-columnar&metric=alert_status&token=55129ac721eacd76417f57921368ed587ad8339d)](https://sonarcloud.io/summary/new_code?id=KNIME_knime-core-columnar)

This repository is maintained by the [KNIME Team Rakete](mailto:team-rakete@knime.com).

The Columnar Table Backend stores tables in the Apache Arrow format and enables more efficient access to KNIME tables.

## Content

This repository contains the source code for the Columnar Table Backend.
The code is organized as follows:

* _org.knime.core.columnar_: Columnar table framework definition
* _org.knime.core.columnar.arrow_: Apache Arrow implementation of the columnar table framework
* _org.knime.core.data.columnar_: Table Backend implementation based on the columnar table framework

## Development Notes

You can find instructions on how to work with our code or develop extensions for KNIME Analytics Platform in the _knime-sdk-setup_ repository on [BitBucket](https://bitbucket.org/KNIME/knime-sdk-setup) or [GitHub](http://github.com/knime/knime-sdk-setup).

## Join the Community

* [KNIME Forum](https://forum.knime.com/)
