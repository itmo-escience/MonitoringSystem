package ifmo.escience.dapris.monitoring.computationsMonitor;
import ifmo.escience.dapris.monitoring.computationsMonitor.StateStructures.ClusterState;

import java.util.List;

/**
 * Created by Pavel Smirnov
 */
public interface IStateDataProvider{
    public ClusterState GetData();
}
