package dbfileorga;

public class test {
    public static void main(String[] args){
        MitgliederDB db = new MitgliederDB(false);
        System.out.println(db);
        //System.out.println(db.read(32));
        db.delete(2);
        db.insert(new Record("65;1;12;Kaps;Aes;22.11.92;01.07.99;130;25"));
        System.out.println(db);
    }
}
