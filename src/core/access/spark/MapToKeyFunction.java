package core.access.spark;

import core.access.iterator.IteratorRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by qui on 7/1/15.
 */
public class MapToKeyFunction implements PairFunction<Tuple2<LongWritable, IteratorRecord>, String, String> {

    private static final long serialVersionUID = 1L;

    int joinAttr;
    public MapToKeyFunction(int joinAttribute) {
        joinAttr = joinAttribute;
    }
    @Override
    public Tuple2<String, String> call(Tuple2<LongWritable, IteratorRecord> longWritableIteratorRecordTuple2) throws Exception {
        String key = String.valueOf(longWritableIteratorRecordTuple2._2().getLongAttribute(joinAttr));
        String record = longWritableIteratorRecordTuple2._2().getKeyString();
        return new Tuple2<String, String>(key, record);
    }
}
