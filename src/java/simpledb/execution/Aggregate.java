package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import java.util.NoSuchElementException;
import simpledb.common.Type;
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private Aggregator aggregator;
    private OpIterator resultIterator;

    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;

        // Determine the type of the aggregate field
        TupleDesc td = child.getTupleDesc();
        if (td.getFieldType(afield) == Type.INT_TYPE) {
            this.aggregator = new IntegerAggregator(gfield, gfield == Aggregator.NO_GROUPING ? null : td.getFieldType(gfield), afield, aop);
        } else {
            this.aggregator = new StringAggregator(gfield, gfield == Aggregator.NO_GROUPING ? null : td.getFieldType(gfield), afield, aop);
        }
    }

    public int groupField() {
        return gfield;
    }

    public String groupFieldName() {
        return gfield == Aggregator.NO_GROUPING ? null : child.getTupleDesc().getFieldName(gfield);
    }

    public int aggregateField() {
        return afield;
    }

    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(afield);
    }

    public Aggregator.Op aggregateOp() {
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
        child.open();
        while (child.hasNext()) {
            Tuple tuple = child.next();
            aggregator.mergeTupleIntoGroup(tuple);
        }
        resultIterator = aggregator.iterator();
        resultIterator.open();
        super.open();
    }

    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (resultIterator != null && resultIterator.hasNext()) {
            return resultIterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        resultIterator.rewind();
    }

    public TupleDesc getTupleDesc() {
        TupleDesc childTd = child.getTupleDesc();
        if (gfield == Aggregator.NO_GROUPING) {
            return new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{aop.toString() + "(" + childTd.getFieldName(afield) + ")"});
        } else {
            return new TupleDesc(new Type[]{childTd.getFieldType(gfield), Type.INT_TYPE}, new String[]{childTd.getFieldName(gfield), aop.toString() + "(" + childTd.getFieldName(afield) + ")"});
        }
    }

    public void close() {
        child.close();
        resultIterator.close();
        super.close();
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }
}