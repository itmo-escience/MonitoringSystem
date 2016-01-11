package StateStructures;

import org.javers.core.metamodel.annotation.Id;

import java.util.*;

/**
 * Created by Pavel Smirnov
 */
public class Framework {
    @Id
    private String id;
    private String name;
    private String status;
    private String clusterId;
    private List<Task> tasks;
    private List<Executor> executors;
    private List<Pair> resources;
    //private HashMap <String, Object> resources;

    public Framework(){}
    public Framework(String id){
        this.id = id;
        tasks = new ArrayList<>();
    }
    public Framework(String id, String name, String clusterId, String status){
        this(id);
        this.name = name;
        this.clusterId = clusterId;
        this.status = status;
    }

    public String getId(){ return id; }
    public void setId(String id){ this.id=id; }

    public String getName(){ return name; }
    public void setName(String name){ this.name=name; }

    public String getClusterId(){ return clusterId; }
    public void setClusterId(String clusterId){ this.clusterId=clusterId; }

    public String getStatus(){ return status; }
    public void setStatus(String status){ this.status=status; }

    public List<Task> getTasks(){ return tasks; }
    public void setTasks(List<Task> tasks){  this.tasks = tasks; }

    public List<Executor> getExecutors(){ return executors; }
    public void setExecutors(List<Executor> executors){  this.executors = executors; }

    public List<Pair> getResources(){ return resources; }
    public void setResources(List<Pair> resources){ this.resources = resources; }
}
