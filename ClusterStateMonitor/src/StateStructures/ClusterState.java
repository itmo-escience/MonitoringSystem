package StateStructures;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.javers.core.metamodel.annotation.Id;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Pavel Smirnov
 */
@JsonIgnoreProperties({"StartedToDate", "AllTasks"})
public class ClusterState {
    @Id
    private String id;
    private Double started;
    private String name;
    private List<Framework> frameworks = new LinkedList<>();
    private List<Slave> slaves = new LinkedList<>();

    public ClusterState(){}
    public ClusterState(Double started){
        this.started = started;
        this.id = getStartedToDate().toString();
    }
    public ClusterState(String name, Double started){
        this(started);
        this.name = name;
    }

    public void setId(String id){ this.id = id; }
    public String getId(){ return id; }

    public Double getStarted(){ return started; }
    public void setStarted(Double started){ this.started = started; }

    public String getName(){ return name; }
    public void setName(String name){ this.name =name; }

    public List<Framework> getFrameworks(){ return frameworks; }
    public void setFrameworks(List<Framework> frameworks){  this.frameworks = frameworks; }

    public List<Slave> getSlaves(){ return slaves; }
    public void setSlaves(List<Slave> slaves){  this.slaves = slaves; }

    @JsonProperty("AllTasks")
    public List<Task> getAllTasks(){
        return frameworks.stream().flatMap(f -> f.getTasks().stream()).collect(Collectors.toList());
    }

    @JsonProperty("StartedToDate")
    public Date getStartedToDate(){ return  new Date((long) (started * 1000)); }

}
