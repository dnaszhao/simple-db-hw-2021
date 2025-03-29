package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File f;
    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try {
            byte[] data = new byte[BufferPool.getPageSize()];
            RandomAccessFile file = new RandomAccessFile(f, "r");
            file.seek(pid.getPageNumber() * BufferPool.getPageSize());
            file.readFully(data);
            file.close();
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Could not read page " + pid);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(f, "rw")) {
            file.seek(page.getId().getPageNumber() * BufferPool.getPageSize());
            file.write(page.getPageData());
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        for (int i = 0; i < numPages(); i++) {
            HeapPageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                page.markDirty(true, tid);
                return Collections.singletonList(page);
            }
        }

        // No empty slots found, create a new page
        HeapPageId newPid = new HeapPageId(getId(), numPages());
        HeapPage newPage = new HeapPage(newPid, HeapPage.createEmptyPageData());
        newPage.insertTuple(t);
        newPage.markDirty(true, tid);
        writePage(newPage);
        return Collections.singletonList(newPage);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        RecordId rid = t.getRecordId();
        if (rid == null) {
            throw new DbException("Tuple has no RecordId");
        }

        HeapPageId pid = (HeapPageId) rid.getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        page.markDirty(true, tid);
        return new ArrayList<>(Collections.singletonList(page));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator {
        private TransactionId tid;
        private Iterator<Tuple> currentIterator;
        private int currentPageNo;
        private int totalPages;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
            this.currentPageNo = -1;
            this.totalPages = numPages();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            currentPageNo = 0;
            currentIterator = getPageTuples(currentPageNo);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (currentIterator == null) {
                return false;
            }

            if (currentIterator.hasNext()) {
                return true;
            }

            // Move to the next page
            currentPageNo++;
            while (currentPageNo < totalPages) {
                currentIterator = getPageTuples(currentPageNo);
                if (currentIterator.hasNext()) {
                    return true;
                }
                currentPageNo++;
            }

            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException("No more tuples");
            }
            return currentIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            currentIterator = null;
            currentPageNo = -1;
        }

        private Iterator<Tuple> getPageTuples(int pageNumber) throws DbException, TransactionAbortedException {
            HeapPageId pid = new HeapPageId(getId(), pageNumber);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            return page.iterator();
        }
    }
}
