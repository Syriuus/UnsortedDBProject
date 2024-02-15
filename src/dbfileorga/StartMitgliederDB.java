package dbfileorga;

public class StartMitgliederDB {

	public static void main(String[] args) {
			MitgliederDB db = new MitgliederDB(false);
			System.out.println(db);


			// read the a record number e.g. 32 (86;3;13;Brutt;Jasmin;12.12.04;01.01.16;;7,5)
			Record rec = db.read(32);
			System.out.println(rec);

			//find and read a record with a given Mitgliedesnummer e.g 95
			rec = db.read(db.findPos("95"));
			System.out.println(rec);

			//insert Hans Meier
			int newRecNum = db.insert(new Record("122;2;44;Meier;Hans;07.05.01;01.03.10;120;15"));
			System.out.println(db.read(newRecNum));

			//modify (ID95 Steffi Brahms wird zu ID 95 Steffi Bach)
			db.modify(db.findPos("107"), new Record("107;3;13;Traut;Jenny;09.06.00;01.07.13;;15"));
			System.out.println(db);

			//delete the record with Mitgliedsnummer 95
			db.delete(db.findPos("97"));


			db.closeGapsInDB();
			System.out.println(db);

	}

}
