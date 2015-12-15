package StateStructures;

import java.util.TreeMap;

/**
 * Created by Pavel Smirnov
 */
public class HostState {
    private String id;
    private String pid;
    private String hostname;
    private TreeMap<String, Object> resources;
    //private TreeMap<String, Object> metrics;

    public HostState(){

    }

    public HostState(String id, String pid, String hostname){
        this();
        this.id=id;
        this.pid=pid;
        this.hostname = hostname;
    }

    public String getId(){ return id; };
    public String getPid(){ return pid; };
    public String getHostname(){ return hostname; };
    public TreeMap<String, Object> getResources(){ return resources; }
    public void getResources(TreeMap<String, Object> resources){ this.resources=resources; }
    //public TreeMap<String, Object> getMetrics(){ return metrics; }

}
