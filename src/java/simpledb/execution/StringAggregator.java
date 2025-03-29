package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private final Map<Field, Integer> groupCounts;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("StringAggregator only supports COUNT operation");
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groupCounts = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupKey = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        groupCounts.put(groupKey, groupCounts.getOrDefault(groupKey, 0) + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        List<Tuple> tuples = new ArrayList<>();
        TupleDesc td;
        if (gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
        } else {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"groupVal", "aggregateVal"});
        }

        for (Map.Entry<Field, Integer> entry : groupCounts.entrySet()) {
            Tuple tuple = new Tuple(td);
            if (gbfield == NO_GROUPING) {
                tuple.setField(0, new IntField(entry.getValue()));
            } else {
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(entry.getValue()));
            }
            tuples.add(tuple);
        }

        return new TupleIterator(td, tuples);
    }
}
