package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private final Predicate p;
    private OpIterator child;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        this.p = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        return this.p;
    }

    public TupleDesc getTupleDesc() {
        return this.child.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open(); // 确保 Operator 类的状态被正确设置
        child.open();
    }

    public void close() {
        child.close();
        super.close(); // 确保 Operator 类的状态被正确设置
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        while (child.hasNext()) {
            Tuple tuple = child.next();
            if (p.filter(tuple)) {
                return tuple;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (children == null || children.length != 1) {
            throw new IllegalArgumentException("Expected exactly one child operator");
        }
        this.child = children[0];
    }
}
