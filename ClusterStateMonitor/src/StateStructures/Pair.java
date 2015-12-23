package StateStructures;

import org.javers.core.metamodel.annotation.Id;

/**
 * Created by Pavel Smirnov
 */
public class Pair {
    @Id
    private String id;
    private String key;
    private Object value;

    public Pair(){}
    public Pair (String key, Object value, String id){
        this.key=key;
        this.value = value;
        this.id=id;
    }

    public String getId(){ return id; }
    public void setId(String id){ this.id=id; }
    public String getKey(){ return key; }
    public void setKey(String key){ this.key=key; }
    public Object getValue(){ return value; }
    public void setValue(Object value){ this.value = value; }
}
