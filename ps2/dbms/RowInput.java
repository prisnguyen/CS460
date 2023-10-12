/*
 * RowInput.java
 *
 * DBMS Implementation
 */

import java.io.*;
import java.util.*;

/**
 * An input stream with methods that read values from a byte array.
 * It uses a DataInputStream, and it is similar to the TupleInput class 
 * from Berkeley DB's Bind API.
 *
 * We're using our own version so that we can make it easier to perform
 * the types of reads that are needed during unmarshalling.
 */
public class RowInput {
    /* the underlying byte array used by this RowInput object */
    private byte[] bytes;
    
    /* the underlying DataInputStream used by this RowInput object */
    private DataInputStream dataIn;
    
    /* the current offset within the byte array */
    private int currentOffset;
    
    /**
     * Constructs a RowInput object
     */
    public RowInput(byte[] bytes) {
        this.bytes = bytes;
        this.dataIn = new DataInputStream(new ByteArrayInputStream(bytes));
        this.dataIn.mark(0);
        this.currentOffset = 0;
    }
    
    /*
     * Checks the validity of an offset specified by the user,
     * seeing if it is possible to read bytesToRead bytes at that
     * offset in this RowInput's bytes array.
     * 
     * @throws  IllegalArgumentException if either value is negative 
     *          or if offset is too large to read numBytes bytes
     */
    private void checkOffset(int offset, int bytesToRead) {
        if (offset < 0 || offset > this.bytes.length - bytesToRead) {
            String err = "cannot read " + bytesToRead + " bytes "
                       + "at an offset of " + offset + " in a "
                       + "byte array of length " + this.bytes.length;
            throw new IllegalArgumentException(err);
        }
    }
    
    /*
     * Prepares for a read of bytesToRead bytes at the specified offset
     * in this RowInput's bytes array.
     * 
     * @throws  IllegalArgumentException if offset is negative or too large
     */
    private void prepare(int offset, int bytesToRead) throws IOException {
        checkOffset(offset, bytesToRead);
        this.dataIn.reset();
        this.dataIn.skip(offset);
        this.currentOffset = offset;
    }
    
    /**
     * reads a boolean at the specified offset in this RowInput's byte array
     * 
     * @return  the boolean value that was read
     * @throws  IllegalArgumentException if offset is negative or too large
     * @throws  IllegalStateException if the read cannot be performed
     *          for some other reason
     */
    public boolean readBooleanAtOffset(int offset) {
        try {
            this.prepare(offset, 1);
            boolean ret = this.dataIn.readBoolean();
            this.currentOffset += 1;
            return ret;
        } catch(IOException e) {
            throw new IllegalStateException(e.toString());
        }
    }
    
    /**
     * reads a boolean at the current offset in this RowInput's byte array
     * 
     * @return  the boolean value that was read
     * @throws  IllegalStateException if the read cannot be performed
     */
    public boolean readNextBoolean() {
        try {
            return this.readBooleanAtOffset(this.currentOffset);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(e.toString());
        }
    }
    
    /**
     * reads one byte at the specified offset in this RowInput's byte array
     * 
     * @return  the byte that was read
     * @throws  IllegalArgumentException if offset is negative or too large
     * @throws  IllegalStateException if the read cannot be performed
     *          for some other reason
     */
    public byte readByteAtOffset(int offset) {
        try {
            this.prepare(offset, 1);
            byte ret = this.dataIn.readByte();
            this.currentOffset += 1;
            return ret;
        } catch(IOException e) {
            throw new IllegalStateException(e.toString());
        }
    }
    
    /**
     * reads one byte at the current offset in this RowInput's byte array
     * 
     * @return  the byte that was read
     * @throws  IllegalStateException if the read cannot be performed
     */
    public byte readNextByte() {
        try {
            return this.readByteAtOffset(this.currentOffset);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(e.toString());
        }
    }
    
    /**
     * reads a short (a two-byte integer) at the specified offset in this 
     * RowInput's byte array
     * 
     * @return  the short that was read
     * @throws  IllegalArgumentException if offset is negative or too large
     * @throws  IllegalStateException if the read cannot be performed
     *          for some other reason
     */
    public short readShortAtOffset(int offset) {
        try {
            this.prepare(offset, 2);
            short ret = this.dataIn.readShort();
            this.currentOffset += 2;
            return ret;
        } catch(IOException e) {
            throw new IllegalStateException(e.toString());
        }
    }
    
    /**
     * reads a short (a two-byte integer) at the current offset in 
     * this RowInput's byte array
     * 
     * @return  the short that was read
     * @throws  IllegalStateException if the read cannot be performed
     */
    public short readNextShort() {
        try {
            return this.readShortAtOffset(this.currentOffset);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(e.toString());
        }
    }
    
    /**
     * reads an int (a four-byte integer) at the specified offset in this 
     * RowInput's byte array
     * 
     * @return  the integer that was read
     * @throws  IllegalArgumentException if offset is negative or too large
     * @throws  IllegalStateException if the read cannot be performed
     *          for some other reason
     */
    public int readIntAtOffset(int offset) {
        try {
            this.prepare(offset, 4);
            int ret = this.dataIn.readInt();
            this.currentOffset += 4;
            return ret;
        } catch(IOException e) {
            throw new IllegalStateException(e.toString());
        }
    }
    
    /**
     * reads an int (a four-byte integer) at the current offset in 
     * this RowInput's byte array
     * 
     * @return  the integer that was read
     * @throws  IllegalStateException if the read cannot be performed
     */
    public int readNextInt() {
        try {
            return this.readIntAtOffset(this.currentOffset);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(e.toString());
        }
    }

    /**
     * reads a value of type double at the specified offset in this 
     * RowInput's byte array
     * 
     * @return  the double that was read
     * @throws  IllegalArgumentException if offset is negative or too large
     * @throws  IllegalStateException if the read cannot be performed
     *          for some other reason
     */
    public double readDoubleAtOffset(int offset) {
        try {
            this.prepare(offset, 8);
            double ret = this.dataIn.readDouble();
            this.currentOffset += 8;
            return ret;
        } catch(IOException e) {
            throw new IllegalStateException(e.toString());
        }
    }
    
    /**
     * reads a value of type double at the current offset in 
     * this RowInput's byte array
     * 
     * @return  the double that was read
     * @throws  IllegalStateException if the read cannot be performed
     */
    public double readNextDouble() {
        try {
            return this.readDoubleAtOffset(this.currentOffset);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(e.toString());
        }
    }
    
    /**
     * returns a String constructed by reading numBytes bytes at the 
     * specified offset in this RowInput's byte array
     * 
     * @return  the String that was read
     * @throws  IllegalArgumentException if either value is negative 
     *          or if offset is too large to read numBytes bytes
     * @throws  IllegalStateException if the read cannot be performed
     *          for some other reason
     */
    public String readBytesAtOffset(int offset, int numBytes) {
        try {
            this.prepare(offset, numBytes);
            String ret = "";
            for (int i = 0; i < numBytes; i++) {
                ret += (char)this.dataIn.readByte();
                this.currentOffset++;
            }    
            return ret;
        } catch(IOException e) {
            throw new IllegalStateException(e.toString());
        }
    }
    
    /**
     * returns a String constructed by reading numBytes bytes at the 
     * current offset in this RowInput's byte array
     * 
     * @return  the String that was read
     * @throws  IllegalArgumentException if numBytes is negative 
     *          or if it is too large given the current offset
     * @throws  IllegalStateException if the read cannot be performed
     *          for some other reason
     */
    public String readNextBytes(int numBytes) {
        return this.readBytesAtOffset(this.currentOffset, numBytes);
    }
    
    /**
     * Returns a String representation of this RowInput object that 
     * includes the underlying byte array and the current offset.
     *
     * @return  a String for this RowInput
     */
    public String toString() {
        return "byte array: " + Arrays.toString(this.bytes) + "\n"
             + "current offset: " + this.currentOffset;
    }
}