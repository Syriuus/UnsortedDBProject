package dbfileorga;

import java.util.*;

public class MitgliederDB implements Iterable<Record> {

    protected DBBlock db[] = new DBBlock[8];
    private boolean isDbOrdered = false;


    public MitgliederDB(boolean ordered) {
        this();
        this.isDbOrdered = ordered;
        insertMitgliederIntoDB();

    }

    public MitgliederDB() {
        initDB();
    }

    private void initDB() {
        for (int i = 0; i < db.length; ++i) {
            db[i] = new DBBlock();
        }

    }

    private void insertMitgliederIntoDB() {
        MitgliederTableAsArray mitglieder = new MitgliederTableAsArray();
        String mitgliederDatasets[];
        if (isDbOrdered) {
            mitgliederDatasets = mitglieder.recordsOrdered;
        } else {
            mitgliederDatasets = mitglieder.records;
        }
        for (String currRecord : mitgliederDatasets) {
            appendRecord(new Record(currRecord));
        }
    }


    protected int appendRecord(Record record) {
        //search for block where the record should be appended
        int currBlock = getBlockNumOfRecord(getNumberOfRecords());
        int result = db[currBlock].insertRecordAtTheEnd(record);
        if (result != -1) { //insert was successful
            return result;
        } else if (currBlock < db.length) { // overflow => insert the record into the next block
            return db[currBlock + 1].insertRecordAtTheEnd(record);
        }
        return -1;
    }


    @Override
    public String toString() {
        String result = new String();
        for (int i = 0; i < db.length; ++i) {
            result += "Block " + i + "\n";
            result += db[i].toString();
            result += "-------------------------------------------------------------------------------------\n";
        }
        return result;
    }

    /**
     * Returns the number of Records in the Database
     *
     * @return number of records stored in the database
     */
    public int getNumberOfRecords() {
        int result = 0;
        for (DBBlock currBlock : db) {
            result += currBlock.getNumberOfRecords();
        }
        return result;
    }

    /**
     * Returns the block number of the given record number
     *
     * @param recNum the record number to search for
     * @return the block number or -1 if record is not found
     */
    public int getBlockNumOfRecord(int recNum) {
        int recCounter = 0;
        for (int i = 0; i < db.length; ++i) {
            if (recNum <= (recCounter + db[i].getNumberOfRecords())) {
                return i;
            } else {
                recCounter += db[i].getNumberOfRecords();
            }
        }
        return -1;
    }

    private int getRelativeRecNum(int numRecord, int blockNum) {
        for (int i = 0; i < blockNum; i++) {
            numRecord -= db[i].getNumberOfRecords();
        }
        return numRecord;
    }

    public DBBlock getBlock(int i) {
        return db[i];
    }


    /**
     * Returns the record matching the record number
     *
     * @param recNum the term to search for
     * @return the record matching the search term
     */
    public Record read(int recNum) {
		// TODO binary search?
        int counter = 0;
        for (DBBlock b : db) {
            for (Record record : b) {
                counter++;
                if (counter == recNum) return record;
            }
        }
        return null;
    }


    /**
     * Returns the number of the first record that matches the search term
     *
     * @param searchTerm the term to search for
     * @return the number of the record in the DB -1 if not found
     */
    public int findPos(String searchTerm) {
        if (!isDbOrdered) {
            int counter = 0;
            for (DBBlock b : db) {
                for (Record record : b) {
                    counter++;
                    if (record.getAttribute(1).equals(searchTerm)) return counter;
                }
            }
        } else {
            try {
                int max = getNumberOfRecords();
                int min = 1;

                while (min <= max) {
                    int mid = min + (max - min) / 2;
                    int midBlockNumber = getBlockNumOfRecord(mid);
                    String midAttribute = db[midBlockNumber].getRecord(getRelativeRecNum(mid, midBlockNumber)).getAttribute(1);

                    if (midAttribute.equals(searchTerm))
                        return mid;
                    if (Integer.parseInt(midAttribute) < Integer.parseInt(searchTerm))
                        min = mid + 1;
                    else
                        max = mid - 1;
                }
            } catch (NumberFormatException e) {}
        }
        return -1;
    }

    /**
     * Inserts the record into the file and returns the record number
     *
     * @param record
     * @return the record number of the inserted record
     */
    public int insert(Record record) {
        //TODO implement orderedDB Solution
		if(findPos(record.getAttribute(1)) != -1) return -1;
		if(!isDbOrdered){
			int result = -1;
			int blockNumber = 0;
			while (result == -1) {
				result = db[blockNumber].insertRecord(record);
				blockNumber++;
				if (blockNumber > 8) return -1;
			}
			return findPos(record.getAttribute(1));
		}
		else{
            List<String> leftoverRecords = new ArrayList<>();
            String[] blockPosition = getBlockPosition(record).split(",");
            int blockNumber = Integer.parseInt(blockPosition[0]);
            int recordNumberInBlock = Integer.parseInt(blockPosition[1]);



            for (int i = blockNumber; i < db.length; i++) {
                DBBlock block = db[i];
                DBBlock newBlock = new DBBlock();
                for (int k = 1; k <= block.getNumberOfRecords(); k++) {
                    Record r = block.getRecord(k);

                    leftoverRecords.removeIf(s -> newBlock.insertRecord(new Record(s)) != -1);

                    if (i == blockNumber && k == recordNumberInBlock) {
                        if (newBlock.insertRecord(record) == -1) {
                            leftoverRecords.add(record.toString());
                        }
                    }
                    if (newBlock.insertRecord(r) == -1) {
                        leftoverRecords.add(r.toString());
                    }
                }
                db[i] = newBlock;
            }

            return findPos(record.getAttribute(1));

		}

    }

    private int getInsertPosition(Record record){
        int maxRecords = getNumberOfRecords();
        int lowerBound = 1;
        int insertPosition = 0;
        while(lowerBound < maxRecords) {
            int middle = lowerBound + (maxRecords - lowerBound) / 2;
            int middleBlockNumber = getBlockNumOfRecord(middle);
            String middleAttribute = db[middleBlockNumber].getRecord(getRelativeRecNum(middle, middleBlockNumber)).getAttribute(1);

            if(Integer.parseInt(middleAttribute) < Integer.parseInt(record.getAttribute(1))) {
                lowerBound = middle + 1;
            } else {
                maxRecords = middle - 1;
            }
        }

        if(lowerBound > maxRecords) {
            lowerBound = maxRecords;
        }

        if(Integer.parseInt(read(lowerBound).getAttribute(1)) < Integer.parseInt(record.getAttribute(1))) {
            insertPosition = lowerBound + 1;
        } else {
            insertPosition = maxRecords;
        }
        return insertPosition;
    }

    private String getBlockPosition(Record record){
        int insertPosition = getInsertPosition(record);
        int blockNumber = 0;
        int recordNumberInBlock = 0;
        int counter = 0;

        for (DBBlock block : db) {
            if (insertPosition - block.getNumberOfRecords() < 0) {
                blockNumber = counter;
                recordNumberInBlock = insertPosition;
                break;
            }
            insertPosition -= block.getNumberOfRecords();
            counter++;
            System.out.println(counter + "  " + insertPosition);
        }
        return blockNumber + "," + recordNumberInBlock;
    }

    /**
     * Deletes the record specified
     *
     * @param numRecord number of the record to be deleted
     */
    public void delete(int numRecord) {
        //TODO implement
        int blockNumOfRecord = getBlockNumOfRecord(numRecord);
        db[blockNumOfRecord].deleteRecord(getRelativeRecNum(numRecord, blockNumOfRecord));
    }


    /**
     * Replaces the record at the specified position with the given one.
     *
     * @param numRecord the position of the old record in the db
     * @param newRecord the new record
     */
    public void modify(int numRecord, Record newRecord) {
        delete(numRecord);
        insert(newRecord);
    }

    private void closeGapsInDB() {
        List<String> dbContent = new ArrayList<>();

        for (DBBlock b : db) {
            for (Record record : b) {
                dbContent.add(record.toString());
            }
            b.delete();
        }
        for (String record : dbContent) {
            appendRecord(new Record(record));
        }
    }


    @Override
    public Iterator<Record> iterator() {
        return new DBIterator();
    }

    private class DBIterator implements Iterator<Record> {

        private int currBlock = 0;
        private Iterator<Record> currBlockIter = db[currBlock].iterator();

        public boolean hasNext() {
            if (currBlockIter.hasNext()) {
                return true;
            } else if (currBlock < (db.length - 1)) { //continue search in the next block
                return db[currBlock + 1].iterator().hasNext();
            } else {
                return false;
            }
        }

        public Record next() {
            if (currBlockIter.hasNext()) {
                return currBlockIter.next();
            } else if (currBlock < db.length) { //continue search in the next block
                currBlockIter = db[++currBlock].iterator();
                return currBlockIter.next();
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }


}
