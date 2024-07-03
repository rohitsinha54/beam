package org.apache.beam.sdk.metrics;

/** Standard collection of metrics used to record source and sinks information for
 * lineage tracking.*/
public class Lineage {


  private static final String LINEAGE_NAMESPACE = "lineage";

  private static final String SOURCE_METRIC_NAME = "sources";
  private static final String SINK_METRIC_NAME = "sinks";

  private static final StringSet SOURCES =
      Metrics.stringSet(LINEAGE_NAMESPACE, SOURCE_METRIC_NAME);
  private static final StringSet SINKS =
      Metrics.stringSet(LINEAGE_NAMESPACE, SINK_METRIC_NAME);

  /** String set representing sources. */
  public static StringSet getSources() {
    return SOURCES;
  }

  /** String set representing sinks. */
  public static StringSet getSinks() {
    return SINKS;
  }
}
