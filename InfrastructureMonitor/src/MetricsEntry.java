package ifmo.escience.dapris.monitoring.infrastructureMonitor;

import java.util.Date;
import java.util.SortedMap;

/**
 * Created by Pavel Smirnov
 */
public class MetricsEntry {
    public Double timestamp;
    //public LocalDateTime date;
    //@JsonFormat(shape=JsonFormat.Shape.STRING, pattern="yyyy-MM-dd HH:mm a z")
    public Date date;
    public String hostname;
    public SortedMap<String, Object> metrics;

    public MetricsEntry(Double timestamp, String hostname, SortedMap<String, Object> metrics){
        this.timestamp = timestamp;
        this.hostname = hostname;
        this.metrics = metrics;
    }
    public MetricsEntry(){

    }
//    public String getHostname(){return  hostname;}
//    public void setHostname(String hostame){this.hostname=hostname;}
//
//    public Double getTimestamp(){return timestamp;}
//    public void setHostname(String timestamp){this.hostname=hostname;}
}
