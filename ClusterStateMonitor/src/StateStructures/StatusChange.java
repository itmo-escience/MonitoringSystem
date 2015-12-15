package StateStructures;

import java.util.Date;
import java.util.HashMap;

/**
 * Created by Pavel Smirnov
 */
public class StatusChange {
    public String status;
    public Date timestamp;

    public void setStatus(String value){ this.status = value; }
    public String getStatus(){ return status; }

    public void setTimestamp(Date date){ this.timestamp = date; }
    public Date getTimestamp(){ return timestamp; }

    public StatusChange(){}
    public StatusChange(String status, Date timestamp){
        this.status = status;
        this.timestamp = timestamp;
    }

}
