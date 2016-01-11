package StateStructures;

import org.javers.core.metamodel.annotation.Id;

/**
 * Created by Pavel Smirnov
 */
public class Pair {
    @Id
    private String uniqueId;
    private String key;
    private Object value;

    public Pair(){}
    public Pair (String key, Object value, String uniqueId){
        this.key=key;
        this.value = value;
        this.uniqueId = uniqueId;
    }

    public String getUniqueId(){ return uniqueId; }
    public void setUniqueId(String uniqueId){ this.uniqueId = uniqueId; }
    public String getKey(){ return key; }
    public void setKey(String key){ this.key=key; }
    public Object getValue(){ return value; }
    public void setValue(Object value){ this.value = value; }
}
