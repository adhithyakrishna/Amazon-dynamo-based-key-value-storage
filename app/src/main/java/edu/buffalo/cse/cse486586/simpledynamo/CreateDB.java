package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

class createDB extends SQLiteOpenHelper {
    private static final String TableName = "Gamble";
    private static final String firstColumn = "key";
    private static final String secondColumn = "value";

    private static final String createTable = "CREATE TABLE "
            + TableName + "("
            + firstColumn + " TEXT PRIMARY KEY NOT NULL,"
            + secondColumn + " TEXT NOT NULL);";
    private static final String deleteTable = "DROP TABLE IF EXISTS " + TableName;

    createDB(Context context) {
        super(context, "PA4_Dynamo", null, 1);
    }

    @Override
    public void onCreate(SQLiteDatabase sqldb) {
        sqldb.execSQL(createTable);
    }

    @Override
    public void onUpgrade(SQLiteDatabase sqldb, int oldVersion, int newVersion) {
        sqldb.execSQL(deleteTable);
        onCreate(sqldb);
    }

    public long insert(ContentValues values) {
        long result = -1;

        SQLiteDatabase db = this.getWritableDatabase();

        result = db.insertWithOnConflict(TableName, "", values, SQLiteDatabase.CONFLICT_REPLACE);

        return result;
    }

    public Cursor getData(String key) {
        SQLiteDatabase db = this.getReadableDatabase();
        return db.query(TableName, null, "key=?", new String[]{key}, null, null, null);
    }

    public Cursor getAll() {
        SQLiteDatabase db = this.getReadableDatabase();
        return db.query(TableName, null, null, null, null, null, null);
    }

    public int delectCurrentValue(String value) {
        SQLiteDatabase db = this.getWritableDatabase();
        return db.delete(TableName, "key=?", new String[]{value});
    }
}
