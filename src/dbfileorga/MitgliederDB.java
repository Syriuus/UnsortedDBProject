package dbfileorga;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class MitgliederDB implements Iterable<Record>
{

	protected DBBlock db[] = new DBBlock[8];
	private boolean isDbOrdered = false;


	public MitgliederDB(boolean ordered){
		this();
		this.isDbOrdered = ordered;
		insertMitgliederIntoDB();

	}

	public MitgliederDB(){
		initDB();
	}

	private void initDB() {
		for (int i = 0; i<db.length; ++i){
			db[i]= new DBBlock();
		}

	}

	private void insertMitgliederIntoDB() {
		MitgliederTableAsArray mitglieder = new MitgliederTableAsArray();
		String mitgliederDatasets[];
		if (isDbOrdered){
			mitgliederDatasets = mitglieder.recordsOrdered;
		}else{
			mitgliederDatasets = mitglieder.records;
		}
		for (String currRecord : mitgliederDatasets ){
			appendRecord(new Record(currRecord));
		}
	}


	protected int appendRecord(Record record){
		//search for block where the record should be appended
		int currBlock = getBlockNumOfRecord(getNumberOfRecords());
		int result = db[currBlock].insertRecordAtTheEnd(record);
		if (result != -1 ){ //insert was successful
			return result;
		}else if (currBlock < db.length) { // overflow => insert the record into the next block
			return db[currBlock+1].insertRecordAtTheEnd(record);
		}
		return -1;
	}


	@Override
	public String toString(){
		String result = new String();
		for (int i = 0; i< db.length ; ++i){
			result += "Block "+i+"\n";
			result += db[i].toString();
			result += "-------------------------------------------------------------------------------------\n";
		}
		return result;
	}

	/**
	 * Returns the number of Records in the Database
	 * @return number of records stored in the database
	 */
	public int getNumberOfRecords(){
		int result = 0;
		for (DBBlock currBlock: db){
			result += currBlock.getNumberOfRecords();
		}
		return result;
	}

	/**
	 * Returns the block number of the given record number
	 * @param recNum the record number to search for
	 * @return the block number or -1 if record is not found
	 */
	public int getBlockNumOfRecord(int recNum){
		int recCounter = 0;
		for (int i = 0; i< db.length; ++i){
			if (recNum <= (recCounter+db[i].getNumberOfRecords())){
				return i ;
			}else{
				recCounter += db[i].getNumberOfRecords();
			}
		}
		return -1;
	}

	private int getRelativeRecNum(int numRecord, int blockNum) {
		for(int i = 0; i < blockNum; i++) {
			numRecord -= db[i].getNumberOfRecords();
		}
		return numRecord;
	}

	public DBBlock getBlock(int i){
		return db[i];
	}


	/**
	 * Returns the record matching the record number
	 * @param recNum the term to search for
	 * @return the record matching the search term
	 */
	public Record read(int recNum){
		//TODO sorted DB Search -> binary search
		int counter = 0;
		for(DBBlock b : db){
			for(Record record : b){
				counter++;
				if(counter == recNum) return record;
			}
		}
		return null;
	}

	/**
	 * Returns the number of the first record that matches the search term
	 * @param searchTerm the term to search for
	 * @return the number of the record in the DB -1 if not found
	 */
	public int findPos(String searchTerm){
		//TODO implement ordered serach -> binary search
		int counter = 0 ;
		for(DBBlock b : db){
			for(Record record : b){
				counter++;
				if(record.getAttribute(1).equals(searchTerm)) return counter;
			}
		}
		return -1;
	}

	/**
	 * Inserts the record into the file and returns the record number
	 * @param record
	 * @return the record number of the inserted record
	 */
	public int insert(Record record){
		//TODO implement orderedDB Solution
		int result = -1;
		int blockNumber = 0;
		while(result == -1){
			result = db[blockNumber].insertRecord(record);
			blockNumber++;
			if(blockNumber > 8) return -1;
		}
		return findPos(record.getAttribute(1));
	}

	/**
	 * Deletes the record specified
	 * @param numRecord number of the record to be deleted
	 */
	public void delete(int numRecord){
		//TODO implement
		int blockNumOfRecord = getBlockNumOfRecord(numRecord);
		db[blockNumOfRecord].deleteRecord(getRelativeRecNum(numRecord, blockNumOfRecord));
	}


	/**
	 * Replaces the record at the specified position with the given one.
	 * @param numRecord the position of the old record in the db
	 * @param newRecord the new record
	 *
	 */
	public void modify(int numRecord, Record newRecord){
		if(newRecord.length() <= read(numRecord).length()) {
			int blockNumOfRecord = getBlockNumOfRecord(numRecord);
			db[blockNumOfRecord].modifyRecord(getRelativeRecNum(numRecord, blockNumOfRecord), newRecord);
		} else {
			delete(numRecord);
			insert(newRecord);
		}


	}

	private void closeGapsInDB(){
		List<String> dbContent = new ArrayList<>();

		for(DBBlock b : db){
			for(Record record : b){
				dbContent.add(record.toString());
			}
			b.delete();
		}
		for (String record : dbContent ){
			appendRecord(new Record(record));
		}
	}


	@Override
	public Iterator<Record> iterator() {
		return new DBIterator();
	}

	private class DBIterator implements Iterator<Record> {

		private int currBlock = 0;
		private Iterator<Record> currBlockIter= db[currBlock].iterator();

		public boolean hasNext() {
			if (currBlockIter.hasNext()){
				return true;
			} else if (currBlock < (db.length-1)) { //continue search in the next block
				return db[currBlock+1].iterator().hasNext();
			}else{
				return false;
			}
		}

		public Record next() {
			if (currBlockIter.hasNext()){
				return currBlockIter.next();
			}else if (currBlock < db.length){ //continue search in the next block
				currBlockIter= db[++currBlock].iterator();
				return currBlockIter.next();
			}else{
				throw new NoSuchElementException();
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}


}
