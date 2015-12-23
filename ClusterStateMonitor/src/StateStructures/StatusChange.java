package StateStructures;

import org.javers.core.metamodel.annotation.Id;

import java.util.Date;
import java.util.HashMap;

/**
 * Created by Pavel Smirnov
 */
public class StatusChange {

    @Id
    public String id;
    public Date timestamp;
    public String status;

    public StatusChange(){}
    public StatusChange(String id, String status, Date timestamp){
        this.id = id;
        this.status = status;
        this.timestamp = timestamp;
    }

    public void setId(String id){ this.id = id; }
    public String getId(){ return id; }

    public void setStatus(String value){ this.status = value; }
    public String getStatus(){ return status; }

    public void setTimestamp(Date date){ this.timestamp = date; }
    public Date getTimestamp(){ return timestamp; }
}
