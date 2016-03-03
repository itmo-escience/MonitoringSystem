package ifmo.escience.dapris.monitoring.computationsMonitor.StateStructures;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.javers.core.metamodel.annotation.Id;

import java.util.Date;

/**
 * Created by Pavel Smirnov
 */
@JsonIgnoreProperties({"Timestamp"})
public class StatusChange implements java.io.Serializable {

    @Id
    private String id;
    private Double time;
    private String status;

    public StatusChange(){}
    public StatusChange(String id, String status, Double time){
        this.id = id;
        this.status = status;
        this.time = time;
    }

    public void setId(String id){ this.id = id; }
    public String getId(){ return id; }

    public void setTime(Double time){ this.time = time; }
    public Double getTime(){ return time; }

    public void setStatus(String value){ this.status = value; }
    public String getStatus(){ return status; }

    @JsonProperty("Timestamp")
    public Date getTimeStamp(){ return  new Date((long) (time * 1000)); }
}
