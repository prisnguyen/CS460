/*
 * InsertRow.java
 *
 * DBMS Implementation
 */

import java.io.*;
import java.util.Arrays;

/**
 * A class that represents a row that will be inserted in a table in a
 * relational database.
 *
 * This class contains the code used to marshall the values of the
 * individual columns to a single key-value pair.
 */
public class InsertRow {
	private Table table;           // the table in which the row will be inserted
	private Object[] columnVals;   // the column values to be inserted
	private RowOutput keyBuffer;   // buffer for the marshalled row's key
	private RowOutput valueBuffer; // buffer for the marshalled row's value
	private int[] offsets;         // offsets for header of marshalled row's value
    
	/** Constants for special offsets **/
	/** The field with this offset has a null value. */
	public static final int IS_NULL = -1;
    
	/** The field with this offset is a primary key. */
	public static final int IS_PKEY = -2;
    
	/**
	 * Constructs an InsertRow object for a row containing the specified
	 * values that is to be inserted in the specified table.
	 *
	 * @param  t  the table
	 * @param  values  the column values for the row to be inserted
	 */
	public InsertRow(Table table, Object[] values) {
		this.table = table;
		this.columnVals = values;
		this.keyBuffer = new RowOutput();
		this.valueBuffer = new RowOutput();
        
		// Note that we need one more offset than value,
		// so that we can store the offset of the end of the record.
		this.offsets = new int[values.length + 1];
	}
    
	/**
	 * Takes the collection of values for this InsertRow
	 * and marshalls them into a key/value pair.
	 * 
	 * (Note: We include a throws clause because this method will use 
	 * methods like writeInt() that the RowOutput class inherits from 
	 * DataOutputStream, and those methods could in theory throw that 
	 * exception. In reality, an IOException should *not* occur in the
	 * context of our RowOutput class.)
	 */
	public void marshall() throws IOException {
		/* 
		 * PS 2: Implement this method. 
		 * 
		 * Feel free to also add one or more private helper methods
		 * to do some of the work (e.g., to fill in the offsets array
		 * with the appropriate offsets).
		 */
		Column PriCol=this.table.primaryKeyColumn();
		int primInd=PriCol.getIndex();

		if(PriCol != null) {
			int Type=PriCol.getType();
			switch (Type){
			case 0:
				this.keyBuffer.writeInt((int)this.columnVals[primInd]);
				break;
			case 1:
				this.keyBuffer.writeDouble((double)this.columnVals[primInd]);
				break;
			default:
				this.keyBuffer.writeBytes((String)this.columnVals[primInd]);
				break;
			}
		}

		int i;
		int offrunner = this.offsets.length*2;

		for(i = 0; i < this.columnVals.length; i++){
			Column col = table.getColumn(i);
			int type = col.getType();
			if(i == primInd){
				this.offsets[i]=-2;
			} else if (columnVals[i] == null){
				this.offsets[i] = -1;
			} else {
				this.offsets[i] = offrunner;
				switch(type) {
				case 0:
					offrunner += 4;
					break;
				case 1:
					offrunner += 8;
					break;
				default:
					offrunner += ((String) columnVals[i]).length();
				}
			}
		}

		offsets[i] = offrunner;
		for(i = 0; i < offsets.length; i++){
			valueBuffer.writeShort(offsets[i]);
		}
		for(i = 0;i < columnVals.length; i++){
			if (columnVals[i] != null && i != primInd){
				Column col = table.getColumn(i);
				int type = col.getType();
				switch(type){
				case 0:
					valueBuffer.writeInt((int)columnVals[i]);
					break;
				case 1:
					valueBuffer.writeDouble((double)columnVals[i]);
					break;
				default:
					valueBuffer.writeBytes((String)columnVals[i]);
				}

			}
		}		    
	}
        
	/**
	 * Returns the RowOutput used for the key portion of the marshalled row.
	 *
	 * @return  the key's RowOutput
	 */
	public RowOutput getKeyBuffer() {
		return this.keyBuffer;
	}
    
	/**
	 * Returns the RowOutput used for the value portion of the marshalled row.
	 *
	 * @return  the value's RowOutput
	 */
	public RowOutput getValueBuffer() {
		return this.valueBuffer;
	}
    
	/**
	 * Returns a String representation of this InsertRow object. 
	 *
	 * @return  a String for this InsertRow
	 */
	public String toString() {
		return "offsets: " + Arrays.toString(this.offsets)
			+ "\nkey buffer: " + this.keyBuffer
			+ "\nvalue buffer: " + this.valueBuffer;
	}
}