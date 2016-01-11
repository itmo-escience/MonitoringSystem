package StateStructures;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.javers.core.metamodel.annotation.DiffIgnore;
import org.javers.core.metamodel.annotation.Id;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Pavel Smirnov
 */
@JsonIgnoreProperties({"StartedAsDate", "AllTasks"})
public class ClusterState {
    @Id
    private String id;
    private Double startedEpoch;
    private String started;

    @DiffIgnore
    private String updated;

    private String clusterName;
    private List<Framework> frameworks = new LinkedList<>();
    private List<Slave> slaves = new LinkedList<>();

    public ClusterState(){}
    public ClusterState(Double started, String clusterName){
        this.startedEpoch = started;
        this.clusterName = clusterName;
        this.id = String.valueOf(getStartedAsDate().hashCode());
        this.started = getStartedAsDate().toString();
        this.updated = new Date().toString();
    }

    public void setId(String id){ this.id = id; }
    public String getId(){ return id; }

    public void setStartedEpoch(Double started){ this.startedEpoch = started; }
    public Double getStartedEpoch(){ return startedEpoch; }

    public void setStarted(String started){ this.started = started; }
    public String getStarted(){ return started; }

    public void setUpdated(String updated){ this.updated = updated; }
    public String getUpdated(){ return updated; }

    public String getClusterName(){ return clusterName; }
    public void setClusterName(String clusterName){ this.clusterName = clusterName; }

    public List<Framework> getFrameworks(){ return frameworks; }
    public void setFrameworks(List<Framework> frameworks){  this.frameworks = frameworks; }

    public List<Slave> getSlaves(){ return slaves; }
    public void setSlaves(List<Slave> slaves){  this.slaves = slaves; }

    @JsonProperty("AllTasks")
    public List<Task> getAllTasks(){
        return frameworks.stream().flatMap(f -> f.getTasks().stream()).collect(Collectors.toList());
    }

    @JsonProperty("StartedAsDate")
    public Date getStartedAsDate(){ return  new Date((long) (startedEpoch * 1000)); }

}
