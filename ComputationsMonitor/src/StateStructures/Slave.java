package ifmo.escience.dapris.monitoring.computationsMonitor.StateStructures;

import org.javers.core.metamodel.annotation.Id;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Pavel Smirnov
 */
public class Slave {
    @Id
    private String id;
    private String pid;
    private String hostname;
    private List <Pair> resources;

    public Slave(){

    }

    public Slave(String id, String pid, String hostname){
        this();
        this.id=id;
        this.pid=pid;
        this.hostname = hostname;
    }

    public String getId(){ return id; };
    public void setId(String id){ this.id=id; };
    public String getPid(){ return pid; };
    public void setPid(String pid){ this.pid=pid; };
    public String getHostname(){ return hostname; };
    public void setHostname(String hostname){ this.hostname=hostname; };

    public List <Pair> getResources(){ return resources; }
    public void setResources(List<Pair> resources){ this.resources=resources; }

    public Map<String, Object> getResourceMap(){
        Map<String, Object> ret = new HashMap<String, Object>();
        for(Pair pair : getResources())
            ret.put(pair.getKey(), pair.getValue());
        return ret;
    }
    //public TreeMap<String, Object> getMetrics(){ return metrics; }

}
