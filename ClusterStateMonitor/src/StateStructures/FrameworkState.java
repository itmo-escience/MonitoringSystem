package StateStructures;

import org.javers.core.metamodel.annotation.Id;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by Pavel Smirnov
 */
public class FrameworkState {
    @Id
    private String id;
    private String name;
    private String status;
    private List<TaskState> taskStates;
    private ClusterState clusterState;
    private TreeMap<String, Object> resources;

    public  FrameworkState(){}
    public  FrameworkState(String id){
        this.id = id;
        taskStates = new ArrayList<>();
    }
    public  FrameworkState(String id, String name, ClusterState clusterState, String status){
        this(id);
        this.name = name;
        this.clusterState = clusterState;
        this.status = status;
    }

    public String getID(){ return id; }
    public void setID(String id){ this.id =id; }

    public String getName(){ return name; }
    public void setName(String name){ this.name=name; }

    public String getStatus(){ return status; }
    public void setStatus(String status){ this.status=status; }

    public List<StateStructures.TaskState> getTaskStates(){ return taskStates; }
    public void setTaskStates(List<TaskState> taskStates){  this.taskStates = taskStates; }

    public TreeMap<String, Object> getResources(){ return resources; }
    public void setResources(Map<String, Object> resources){ this.resources = new TreeMap<>(resources); }
}
