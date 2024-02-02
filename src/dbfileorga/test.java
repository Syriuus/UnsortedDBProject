package dbfileorga;

public class test {
    public static void main(String[] args){
        MitgliederDB db = new MitgliederDB(false);
        System.out.println(db);
        System.out.println(db.read(32));
    }
}
