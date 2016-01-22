package ifmo.escience.dapris.monitoring.clusterStateMonitor;
import ifmo.escience.dapris.monitoring.clusterStateMonitor.StateStructures.ClusterState;

import java.util.List;

/**
 * Created by Pavel Smirnov
 */
public interface IStateDataProvider{
    public ClusterState GetData();
}
