package StateStructures;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.javers.core.metamodel.annotation.Id;

import java.util.*;

/**
 * Created by Pavel Smirnov
 */
@JsonIgnoreProperties({"Started","Finished", "Status"})
public class Task {
    @Id
    private String id;
    //private Slave slave;
    private String slaveId;
    private String frameworkId;
    private List<StatusChange> statusChanges = new ArrayList<StatusChange>();
    private List<Pair> resources;

    public Task(){}
    public Task(String id){
        id=id;
    }

    public Task(String id, String frameworkId, String slaveId){
        this.id=id;
        this.frameworkId = frameworkId;
        this.slaveId = slaveId;
    }

    public void setId(String id){ this.id=id; }
    public String getId(){ return id; }
    //public Slave getSlave(){ return slave; }
    //public void setSlave(Slave slave){ this.slave=slave; }
    public String getSlaveId(){ return slaveId; }
    public void setSlaveId(String slaveId){ this.slaveId=slaveId; }



    public List<StatusChange> getStatusChanges(){ return statusChanges; }
    public void setStatusChanges(List<StatusChange> value){ this.statusChanges=value; }

    public String getFrameworkId(){ return this.frameworkId; }
    public void setFrameworkId(String frameworkId){ this.frameworkId=frameworkId; }

    public List<Pair> getResources(){ return resources; }
    public void setResources(List<Pair> resources){ this.resources = resources; }

    @JsonProperty("Status")
    public String getStatus(){ return statusChanges.get(statusChanges.size()-1).getStatus(); }

    @JsonProperty("Started")
    public Date getStarted(){
        if(statusChanges.size()>0)
            return statusChanges.get(0).getTimeStamp();
        return null;
    }

    @JsonProperty("Finished")
    public Date getFinished(){
        if(statusChanges.size()>1)
            return statusChanges.get(statusChanges.size()-1).getTimeStamp();
        return null;
    }
}
