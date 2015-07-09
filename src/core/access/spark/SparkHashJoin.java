package core.access.spark;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

import scala.Tuple2;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

public class SparkHashJoin<K,V> implements Serializable{

	private static final long serialVersionUID = 1L;
    private Multimap<K, V> buildPhase;
    private int firstRelation;
    long count;
    long start;
    long end;
    
    private List<Tuple2<V,V>> joinResults;

    public void initialize(int firstRelation) {
        buildPhase = ArrayListMultimap.create();
        joinResults = Lists.newArrayList();
        this.firstRelation = firstRelation;
        count = 0;
        start = System.currentTimeMillis();
    }

    public boolean add(int relationId, K joinKey, V record) throws RuntimeException {
        if (relationId == firstRelation) 
        	buildPhase.put(joinKey, record);
        else {
            if (end == 0) {
                end = System.currentTimeMillis();
                System.out.println("INFO: join scan and probe time "+(end-start));
            }
        	Collection<V> curRecords = buildPhase.get(joinKey);
            if (curRecords != null) {
                for (V r : curRecords)
                    count++;
                    //joinResults.add(new Tuple2<V, V>(r,record));
            }
        }
        return false; // we always return false as we know the joins results only at the very end
    }

    public List<Tuple2<V,V>> getJoinResults(){
        System.out.println("JOIN: count "+count);
        return joinResults;
    }

    public void clear(){
        buildPhase = null;
        buildPhase = ArrayListMultimap.create();
        joinResults = null;
        joinResults = Lists.newArrayList();
        Runtime.getRuntime().gc();
    }
}