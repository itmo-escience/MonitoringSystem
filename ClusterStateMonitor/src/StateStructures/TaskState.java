package StateStructures;

import org.javers.core.metamodel.annotation.Id;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by Pavel Smirnov
 */
public class TaskState {
    @Id
    public String ID;
    public String slaveID;
    private HashMap<String, Double> requirements;
    private String status;
    public List<StatusChange> statusChanges;
    private FrameworkState frameworkState;
    private TreeMap<String, Object> resources;

    public TaskState(){}
    public TaskState(String id){
        ID=id;
    }

    public TaskState(String id, FrameworkState frameworkState, String status, String slaveID){
        ID=id;
        this.frameworkState = frameworkState;
        this.status = status;
        this.slaveID = slaveID;
    }

    public String getID(){ return ID; }
    public String getSlaveID(){ return slaveID; }

    public String getStatus(){ return status; }
    public void setStatus(String status){ this.status=status; }

    public List<StatusChange> getStatusChanges(){ return statusChanges; }
    public void setStatusChanges(List<StatusChange> value){ this.statusChanges=value; }

    public FrameworkState getFrameworkState(){ return this.frameworkState; }
    public HashMap<String, Double> getRequirements(){ return requirements; }

    public TreeMap<String, Object> getResources(){ return resources; }
    public void setResources(Map<String, Object> resources){ this.resources = new TreeMap<>(resources); }

}
