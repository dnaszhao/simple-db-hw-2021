package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate p;
    private OpIterator child1;
    private OpIterator child2;
    private Tuple currentTuple1;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     *
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
        this.currentTuple1 = null;
    }

    public JoinPredicate getJoinPredicate() {
        return p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        return child1.getTupleDesc().getFieldName(p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        return child2.getTupleDesc().getFieldName(p.getField2());
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child1.open();
        child2.open();
    }

    public void close() {
        super.close();
        child1.close();
        child2.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child1.rewind();
        child2.rewind();
        currentTuple1 = null;
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (currentTuple1 == null && !child1.hasNext()) {
            return null;
        }

        if (currentTuple1 == null) {
            currentTuple1 = child1.next();
        }

        while (true) {
            if (child2.hasNext()) {
                Tuple tuple2 = child2.next();
                if (p.filter(currentTuple1, tuple2)) {
                    TupleDesc tupleDesc = getTupleDesc();
                    Tuple tuple = new Tuple(tupleDesc);
                    for (int i = 0; i < tupleDesc.numFields(); i++) {
                        if (i < currentTuple1.getTupleDesc().numFields()) {
                            tuple.setField(i, currentTuple1.getField(i));
                        } else {
                            tuple.setField(i, tuple2.getField(i - currentTuple1.getTupleDesc().numFields()));
                        }
                    }
                    return tuple;
                }
            } else {
                if (child1.hasNext()) {
                    currentTuple1 = child1.next();
                    child2.rewind();
                } else {
                    return null;
                }
            }
        }
    }

    @Override
    public OpIterator[] getChildren() {
        OpIterator[] opIterators = new OpIterator[2];
        opIterators[0] = child1;
        opIterators[1] = child2;
        return opIterators;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (children == null || children.length != 2) {
            throw new IllegalArgumentException("Expected exactly two child operators");
        }
        this.child1 = children[0];
        this.child2 = children[1];
    }
}
