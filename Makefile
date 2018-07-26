.PHONY: udf deps install restart

DRILL_HOME ?= /usr/local/drill

udf:
	mvn --quiet clean package -DskipTests

install:
	cp target/drill-url-tools*.jar ${DRILL_HOME}/jars/3rdparty && \
	cp deps/galimatias-0.2.0.jar ${DRILL_HOME}/jars/3rdparty && \
	cp deps/icu4j-53.1.jar ${DRILL_HOME}/jars/3rdparty

restart:
	drillbit.sh restart

deps:
	mvn --quiet dependency:copy-dependencies -DoutputDirectory=deps
