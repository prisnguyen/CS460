/*
 * RowOutput.java
 *
 * DBMS Implementation
 */

import java.io.*;
import java.util.*;

/**
 * An output stream with methods that write values into a byte array.
 * It is a subclass of Java's DataOutputStream, and it is similar to 
 * the TupleOutput class from Berkeley DB's Bind API.
 *
 * We're using our own version so that we can make it easier to view
 * and interpret the current contents of the byte array 
 * for debugging purposes.
 */
public class RowOutput extends DataOutputStream {
    /* 
     * We temporarily use this static variable for the ByteArrayOutputStream 
     * that we pass into the DataOutputStream constructor. This is less than 
     * ideal (and not thread-safe), but it stems from the requirement that the
     * call to the superclass constructor must be on the first line of 
     * the constructor for this class. At that point, there is no "this"
     * reference, so we can't assign the ByteArrayOutputStream to
     * the bytes field at that point. Instead, we temporarily assign it
     * to the static variable, and then we assign it to the bytes field
     * after the superclass constructor has returned.
     */
    private static ByteArrayOutputStream baos = null;
    
    /* the underlying ByteArrayOutputStream used by this RowOutput object */
    private ByteArrayOutputStream bytes;
    
    /**
     * Constructs a RowOutput object
     */
    public RowOutput() {
        super(baos = new ByteArrayOutputStream());
        this.bytes = baos;
    }
    
    /**
     * Returns a byte array containing the bytes written to this RowOutput 
     *
     * @return  an array of bytes written to this RowOutput
     */
    public byte[] getBufferBytes() {
        return this.bytes.toByteArray();
    }
    
    /**
     * Returns the number of bytes written to this RowOutput 
     *
     * @return  the number of bytes written
     */
    public int getBufferLength() {
        return this.bytes.size();
    }
    
    /**
     * Returns a String representation of this RowOutput object that 
     * shows the current contents of the underlying byte array.
     *
     * @return  a String for this RowOutput
     */
    public String toString() {
        return Arrays.toString(this.getBufferBytes());
    }
}