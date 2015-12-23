package StateStructures;

import org.javers.core.metamodel.annotation.Id;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Created by Pavel Smirnov
 */

public class ClusterState {
    @Id
    private Date started;
    private String name;
    private List<Framework> frameworks = new LinkedList<>();
    private List<Slave> slaves = new LinkedList<>();

    public ClusterState(){}
    public ClusterState(Date started){
        this.started = started;
    }
    public ClusterState(String name, Date started){
        this(started);
        this.name = name;
    }

    public Date getStarted(){ return started; }
    public void setStarted(Date started){ this.started=started; }

    public String getName(){ return name; }
    public void setName(String name){ this.name =name; }

    public List<Framework> getFrameworks(){ return frameworks; }
    public void setFrameworks(List<Framework> frameworks){  this.frameworks = frameworks; }

    public List<Slave> getSlaves(){ return slaves; }
    public void setSlaves(List<Slave> slaves){  this.slaves = slaves; }

    public List<Task> getAllTasks(){
        return frameworks.stream().flatMap(f -> f.getTasks().stream()).collect(Collectors.toList());
    }


}
