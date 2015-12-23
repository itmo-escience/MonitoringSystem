package StateStructures;

import org.javers.core.metamodel.annotation.Id;
import java.util.TreeMap;

/**
 * Created by Pavel Smirnov
 */
public class Executor {
    @Id
    public String id;
    private String slaveId;
    private TreeMap command;
    private TreeMap<String, Object> resources;
    public Executor(){


    }

    public Executor(String id, String slaveId, TreeMap command, TreeMap resources){
        this.id=id;
        this.slaveId = slaveId;
        this.command = command;
        this.resources = resources;

    }

    public String getId(){ return id; };

    public String getSlaveId(){ return slaveId; };
    public void setSlaveId(String slave){ this.slaveId = slaveId; }

    public TreeMap<String, Object> getResources(){ return resources; }
    public void setResources(TreeMap<String, Object> resources){ this.resources=resources; }
}
