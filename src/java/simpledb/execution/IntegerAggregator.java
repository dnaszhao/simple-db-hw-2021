package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private final Map<Field, Integer> groupCounts;
    private final Map<Field, Integer> groupSums;
    private final Map<Field, Integer> groupMins;
    private final Map<Field, Integer> groupMaxes;

    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groupCounts = new HashMap<>();
        this.groupSums = new HashMap<>();
        this.groupMins = new HashMap<>();
        this.groupMaxes = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupKey = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        int value = ((IntField) tup.getField(afield)).getValue();

        switch (what) {
            case COUNT:
                groupCounts.put(groupKey, groupCounts.getOrDefault(groupKey, 0) + 1);
                break;
            case SUM:
                groupSums.put(groupKey, groupSums.getOrDefault(groupKey, 0) + value);
                break;
            case AVG:
                int count = groupCounts.getOrDefault(groupKey, 0) + 1;
                int sum = groupSums.getOrDefault(groupKey, 0) + value;
                groupCounts.put(groupKey, count);
                groupSums.put(groupKey, sum);
                break;
            case MIN:
                int currentMin = groupMins.getOrDefault(groupKey, Integer.MAX_VALUE);
                groupMins.put(groupKey, Math.min(currentMin, value));
                break;
            case MAX:
                int currentMax = groupMaxes.getOrDefault(groupKey, Integer.MIN_VALUE);
                groupMaxes.put(groupKey, Math.max(currentMax, value));
                break;
            default:
                throw new UnsupportedOperationException("Unsupported aggregation operator: " + what);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        List<Tuple> tuples = new ArrayList<>();
        TupleDesc td;
        if (gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
        } else {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"groupVal", "aggregateVal"});
        }

        for (Map.Entry<Field, Integer> entry : getAggregationMap().entrySet()) {
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

    private Map<Field, Integer> getAggregationMap() {
        switch (what) {
            case COUNT:
                return groupCounts;
            case SUM:
                return groupSums;
            case AVG:
                Map<Field, Integer> avgMap = new HashMap<>();
                for (Map.Entry<Field, Integer> entry : groupCounts.entrySet()) {
                    Field key = entry.getKey();
                    int count = entry.getValue();
                    int sum = groupSums.get(key);
                    avgMap.put(key, sum / count);
                }
                return avgMap;
            case MIN:
                return groupMins;
            case MAX:
                return groupMaxes;
            default:
                throw new UnsupportedOperationException("Unsupported aggregation operator: " + what);
        }
    }
}
