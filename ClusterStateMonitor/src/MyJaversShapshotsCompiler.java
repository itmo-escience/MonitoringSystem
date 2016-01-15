//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

import StateStructures.Pair;
import org.javers.common.collections.Optional;
import org.javers.core.Javers;
import org.javers.core.commit.CommitId;
import org.javers.core.metamodel.object.CdoSnapshot;
import org.javers.core.metamodel.object.InstanceId;
import org.javers.core.metamodel.property.Property;
import org.javers.repository.jql.InstanceIdDTO;
import org.javers.repository.jql.QueryBuilder;

public class MyJaversShapshotsCompiler {
    private static final double EPS = 0.001D;
    private Javers javers;
    private Map<Object, Object> idToEntityMap = new HashMap();

    public MyJaversShapshotsCompiler(Javers javers) {
        this.javers = javers;
    }

    public void clearCache() {
        this.idToEntityMap.clear();
    }

    public Object compileEntityStateFromSnapshot(CdoSnapshot snapshot) {
        this.clearCache();

        try {
            return this.compileEntityInternal(snapshot);
        } catch (InstantiationException | IllegalAccessException var3) {
            var3.printStackTrace();
            return null;
        }
    }

    public Object compileEntityStateForCommitId(InstanceId instanceId, String commitId) {
        this.clearCache();

        try {
            return this.compileEntityForCommitId(instanceId, CommitId.valueOf(commitId));
        } catch (InstantiationException | IllegalAccessException var4) {
            var4.printStackTrace();
            return null;
        }
    }

    public Object compileLatestEntityState(InstanceId instanceId) {
        Optional latestSnapshot = this.javers.getLatestSnapshot(this.convertInstanceIdToDTO(instanceId));
        return latestSnapshot.isPresent()?this.compileEntityStateFromSnapshot((CdoSnapshot)latestSnapshot.get()):null;
    }

    public Object compileLatestEntityStateForEntity(Object entity) {
        return this.compileLatestEntityState(this.javers.idBuilder().instanceId(entity));
    }

    private Object compileEntityInternal(CdoSnapshot snap) throws IllegalAccessException, InstantiationException {
        if(this.idToEntityMap.containsKey(snap.getGlobalId().getCdoId())) {
            return this.idToEntityMap.get(snap.getGlobalId().getCdoId());
        } else {
            Class clientsClass = snap.getManagedType().getBaseJavaClass();
            Object instance = clientsClass.newInstance();
            this.idToEntityMap.put(snap.getGlobalId().getCdoId(), instance);
            Iterator var4 = snap.getProperties().iterator();

            while(true) {
                Property property;
                Object propertyValue;
                Object propertyValueToSet;
                do {
                    if(!var4.hasNext()) {
                        return instance;
                    }

                    property = (Property)var4.next();
                    propertyValue = snap.getPropertyValue(property);
                    propertyValueToSet = propertyValue;
                } while(propertyValue == null);

                Class targetType=null;
                Method exc = null;
                try {
                    exc = this.getSetterForProperty(clientsClass, property);
                    targetType = exc.getParameterTypes()[0];
                } catch (Exception e) {
                    e.printStackTrace();
                }
                    //if(targetType.isAssignableFrom(List.class) || targetType.isAssignableFrom(Set.class)) {
                try{
                        Collection collection = (Collection)propertyValue;
                        HashSet toBeDeleted = new HashSet();
                        Object[] var12 = collection.toArray();
                        int element = var12.length;

                        for(int var14 = 0; var14 < element; ++var14) {
                            Object element1 = var12[var14];
                            if(element1 instanceof InstanceId) {
                                InstanceId instanceId = (InstanceId)element1;
                                Object targetObject = this.compileEntityForCommitId(instanceId, snap.getCommitId());
                                collection.add(targetObject);
                                toBeDeleted.add(element1);
                            }
                        }

                        Iterator var19 = toBeDeleted.iterator();

                        while(var19.hasNext()) {
                            Object var20 = var19.next();
                            collection.remove(var20);
                        }
                    }
                catch (Exception var18) {
                        if(propertyValue instanceof InstanceId) {
                            propertyValueToSet = this.compileEntityForCommitId((InstanceId)propertyValue, snap.getCommitId());
                        }else{ // some primitive type
                            propertyValueToSet = propertyValue;
                            String test = "123";
                        }
                    }

                if(exc!=null)
                    try {
                        exc.invoke(instance, new Object[]{propertyValueToSet});
                    }
                    catch (Exception var18) {
                        var18.printStackTrace();
                    }
            }
        }
    }

    private Object compileEntityForCommitId(InstanceId instanceId, CommitId commitId) throws IllegalAccessException, InstantiationException {
        List snapshots = this.javers.findSnapshots(QueryBuilder.byInstanceId(instanceId, instanceId.getManagedType().getBaseJavaClass()).build());
        double commitToBeSearched = this.commitIdAsDouble(commitId);
        CdoSnapshot snapFound = (CdoSnapshot)snapshots.get(0);
//        Iterator instance = snapshots.iterator();
//
//        while(instance.hasNext()) {
//            CdoSnapshot snap = (CdoSnapshot)instance.next();
//            if(this.commitIdAsDouble(snap.getCommitId()) < commitToBeSearched + 0.001D) {
//                snapFound = snap;
//            }
//        }

        Object instance1 = this.compileEntityInternal(snapFound);
        return instance1;
    }

    private InstanceIdDTO convertInstanceIdToDTO(InstanceId instanceId) {
        return InstanceIdDTO.instanceId(instanceId.getCdoId(), instanceId.getManagedType().getBaseJavaClass());
    }

    private double commitIdAsDouble(CommitId commitId) {
        return Double.parseDouble(commitId.value());
    }

    private Method getSetterForProperty(Class clientsClass, Property property) throws NoSuchFieldException, NoSuchMethodException {
        Field declaredField = clientsClass.getDeclaredField(property.getName());
        Method setter = clientsClass.getDeclaredMethod(this.getSetterNameForProperty(property), new Class[]{declaredField.getType()});
        return setter;
    }

    private String getSetterNameForProperty(Property property) {
        return "set" + property.getName().substring(0, 1).toUpperCase() + property.getName().substring(1);
    }

    public void TestJavers(){

//        CustomTreeMap map = new CustomTreeMap("1"){{
//            put("mem", 4480.0);
//            put("cpus", 10.0);
//            put("disk", 0);
//        }};

        List<Pair> list = new ArrayList<Pair>();
        list.add(new Pair("mem", 4480,"mem"));
        list.add(new Pair("cpus",(double)10,"cpus"));
        list.add(new Pair("disk",(double)0,"disk"));

//            HashSet<IdentifiedObject> map = new HashSet<IdentifiedObject>(){{
//                add(new IdentifiedObject("mem", (double) 4480));
//                add(new IdentifiedObject("cpus",(double)10));
//                add(new IdentifiedObject("disk",(double)0));
//        }};

//        javers.commit("1", list);
//        List<CdoSnapshot> snapshots = javers.findSnapshots(QueryBuilder.byInstanceId("1", list.getClass()).build());
//        if (snapshots.size()>0){
//            CdoSnapshot first = snapshots.get(0);
//            JaversShapshotsCompiler snapCompiler = new JaversShapshotsCompiler(javers);
//            List<StateStructures.Pair> ret = (List<StateStructures.Pair>)snapCompiler.compileEntityStateFromSnapshot(first);
//            Diff diff = javers.compareCollections(list, ret, StateStructures.Pair.class);
//            String test ="123";
//        }

//        while(1==1) {
//
//            try {
//                javers.commit("1", map);
//                Thread.sleep(3000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }

    }

    public void TestJavers2(){
        //javers = JaversBuilder.javers().build();

        //javers.commit("author", new Employee("bob", 31) );

        //javers.commit("author", new Employee("john",25) );


        //Diff changes = javers.findChanges( QueryBuilder.byInstanceId("bob", Employee.class).build() );
        //Object obj =  javers.findSnapshots(QueryBuilder.byInstanceId("bob", Employee.class).build());
        //String str = "123";

    }
}
