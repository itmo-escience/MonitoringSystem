package ifmo.escience.dapris.monitoring.clusterStateMonitor.StateStructures;

import org.javers.core.metamodel.annotation.Id;

import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

/**
 * Created by Pavel Smirnov
 */
public class Executor {
    @Id
    private String id;
    private String slaveId;
    private String frameworkId;
    private String status;
    private List<Pair> command;
    private List<Pair> resources;

    public Executor(){


    }

    public Executor(String id, String frameworkId, String slaveId, String status, List<Pair> command, List<Pair> resources){
        this.id=id;
        this.frameworkId = frameworkId;
        this.slaveId = slaveId;
        this.command = command;
        this.resources = resources;
        this.status = status;
    }

    public String getId(){ return id; };

    public String getSlaveId(){ return slaveId; };
    public void setSlaveId(String slaveId){ this.slaveId = slaveId; }

    public String getFrameworkId(){ return frameworkId; }
    public void setFrameworkId(String frameworkId){ this.frameworkId = frameworkId; }

    public List<Pair> getCommand(){ return command; };
    public void setCommand(List<Pair> command){ this.command = command; }

    public String getStatus(){ return status; }
    public void setStatus(String status){ this.status=status; }

    public List<Pair> getResources(){ return resources; }
    public void setResources(List<Pair> resources){ this.resources=resources; }


}
