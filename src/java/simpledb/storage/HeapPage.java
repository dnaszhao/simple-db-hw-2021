package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Catalog;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte[] header;
    final Tuple[] tuples;
    final int numSlots;

    byte[] oldData;
    private TransactionId preMarkId;
    private boolean isDirty;
    private final Byte oldDataLock= (byte) 0;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        isDirty = false;
        preMarkId = null;
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();

        tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
     @return the number of tuples on this page
     */
    private int getNumTuples() {
        return (int) Math.floor((BufferPool.getPageSize() * 8.0) / (td.getSize() * 8.0 + 1.0));
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        return (int) Math.ceil(getNumTuples() / 8.0);
    }

    /** Return a view of this page before it was modified
     -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        return this.pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {
            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length);
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty HeapPage.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len];
    }

    /**
     * Delete the specified tuple from the page.
     */
    public void deleteTuple(Tuple t) throws DbException {
        RecordId rid = t.getRecordId();
        if (rid == null || !rid.getPageId().equals(pid)) {
            throw new DbException("tuple is not on this page");
        }

        int slot = rid.getTupleNumber();
        if (!isSlotUsed(slot)) {
            throw new DbException("tuple slot is already empty");
        }

        // Mark the slot as empty
        markSlotUsed(slot, false);
        tuples[slot] = null;
    }

    /**
     * Adds the specified tuple to the page.
     */
    public void insertTuple(Tuple t) throws DbException {
        if (!t.getTupleDesc().equals(td)) {
            throw new DbException("tuple desc mismatch");
        }

        if (getNumEmptySlots() == 0) {
            throw new DbException("page is full");
        }

        // Find the first empty slot
        for (int i = 0; i < numSlots; i++) {
            if (!isSlotUsed(i)) {
                markSlotUsed(i, true);
                t.setRecordId(new RecordId(pid, i));
                tuples[i] = t;
                return;
            }
        }

        throw new DbException("no empty slots found");
    }

    /**
     * Marks this page as dirty/not dirty and record the transaction that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        this.isDirty = dirty;
        if (dirty) {
            this.preMarkId = tid;
        } else {
            this.preMarkId = null;
        }
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        return isDirty ? preMarkId : null;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        int res = 0;
        for (int i = 0; i < numSlots; i++) {
            if (!isSlotUsed(i)) {
                res++;
            }
        }
        return res;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        int index = i / 8;
        int loc = i % 8;
        return (header[index] & (1 << loc)) != 0;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        int index = i / 8;
        int loc = i % 8;

        if (value) {
            // Set the bit
            header[index] |= (1 << loc);
        } else {
            // Clear the bit
            header[index] &= ~(1 << loc);
        }
    }

    /**
     * @return an iterator over all tuples on this page
     */
    public Iterator<Tuple> iterator() {
        List<Tuple> usedTuples = new ArrayList<>();
        for (int i = 0; i < tuples.length; i++) {
            if (isSlotUsed(i)) {
                usedTuples.add(tuples[i]);
            }
        }
        return usedTuples.iterator();
    }
}