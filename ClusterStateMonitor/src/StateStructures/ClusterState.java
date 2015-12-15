package StateStructures;

import org.javers.core.metamodel.annotation.Id;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Pavel Smirnov
 */

public class ClusterState {
    @Id
    private Date started;
    private String name;
    private List<FrameworkState> frameworkStates = new LinkedList<>();
    private List<HostState> hostStates = new LinkedList<>();

    public ClusterState(){}
    public ClusterState(Date started){
        this.started = started;
    }
    public ClusterState(String name, Date started){
        this(started);
        this.name = name;
    }

    public Date getID(){ return started; }
    public void setID(Date started){ this.started=started; }

    public String getName(){ return name; }
    public void setName(String name){ this.name =name; }

    public List<FrameworkState> getFrameworkStates(){ return frameworkStates; }
    public void setFrameworkStates(List<FrameworkState> frameworkStates){  this.frameworkStates = frameworkStates; }

    public List<HostState> getHostStates(){ return hostStates; }
    public void setHostStates(List<HostState> hostStates){  this.hostStates = hostStates; }
}
