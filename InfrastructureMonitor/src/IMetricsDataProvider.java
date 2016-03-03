import java.util.List;
import java.util.TreeMap;

/**
 * Created by Pavel Smirnov
 */

public interface IMetricsDataProvider{
        public TreeMap<String, TreeMap<String, Object>> GetData(String clusterName, String[] metricNames);
        public List<String> GetClusterNames();
}
