package org.gbif.crawler.status.service.persistence.handlers;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.gbif.crawler.status.service.pipelines.PipelinesProcessStatus.MetricInfo;

import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;

/**
 * Converts a {@link MetricInfo} to a hstore and viceversa.
 */
public class MetricInfoTypeHandler extends BaseTypeHandler<List<MetricInfo>> {

  private static final String METRIC_INFO_DELIMITER = ":";
  private static final String LIST_DELIMITER = ",";

  @Override
  public void setNonNullParameter(PreparedStatement ps, int i, List<MetricInfo> metrics, JdbcType jdbcType)
      throws SQLException {
    String metricsAsString = metrics.stream()
        .map(metricInfo -> new StringJoiner(METRIC_INFO_DELIMITER).add(metricInfo.getName())
            .add(metricInfo.getValue())
            .toString())
        .collect(Collectors.joining(LIST_DELIMITER));

    ps.setString(i, metricsAsString);
  }

  @Override
  public List<MetricInfo> getNullableResult(ResultSet rs, String columnName) throws SQLException {
    return metricInfoFromString(rs.getString(columnName));
  }

  @Override
  public List<MetricInfo> getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
    return metricInfoFromString(rs.getString(columnIndex));
  }

  @Override
  public List<MetricInfo> getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
    return metricInfoFromString(cs.getString(columnIndex));
  }

  private List<MetricInfo> metricInfoFromString(String hstoreString) throws SQLException {
    if (hstoreString == null) {
      return new ArrayList<>();
    }

    return Arrays.stream(hstoreString.split(LIST_DELIMITER))
        .map(s -> s.split(METRIC_INFO_DELIMITER))
        .map(pieces -> new MetricInfo(pieces[0], pieces[1]))
        .collect(Collectors.toList());
  }

}
