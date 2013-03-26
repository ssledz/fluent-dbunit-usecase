/*
 * Copyright 2013 Sławomir Śledź <slawomir.sledz@sof-tech.pl>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pl.softech.fluent.dbunit.usecase.export;

import java.io.File;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcConnectionPool;
import pl.softech.fluent.dbunit.usecase.Utils;
import pl.softech.fluentdbunit.DBExporter;
import pl.softech.fluentdbunit.DBLoader;
import static pl.softech.fluentdbunit.DBLoader.*;

/**
 *
 * @author Sławomir Śledź <slawomir.sledz@sof-tech.pl>
 */
public class ExportUseCases {

    private static DataSource ds;

    private static void initDb() throws Exception {
        ds = JdbcConnectionPool.create("jdbc:h2:mem:test", "sa", "sa");
        Utils.createTable(ds, "CREATE TABLE author (id identity, name varchar, lastname varchar)");
        Utils.createTable(ds, "CREATE TABLE book (id identity, title varchar, author_id int)");

        DBLoader.forDB(ds).cleanInsert(dataSet(table("author", //
                cols("id", "name", "lastname"), //
                row("1", "Orson", "Card"), //
                row("2", "Terry", "Pratchett"), //
                row("3", "Magdalena", "Kozak"), //
                row("4", "Miroslav", "Žamboch"), //
                row("5", "Terry", "Goodkind") //
                )));

        DBLoader.forDB(ds).cleanInsert(dataSet(table("book", //
                cols("id", "title", "author_id"), //
                row("1", "Ender's Game", "5") //
                )));
    }

    public static void main(String[] args) throws Exception {
        initDb();
        DBExporter.MyDataSet dataSet = DBExporter.forDB(ds).table("author", "book");
        File dir = new File(ExportUseCases.class.getClassLoader().getResource(".").getFile()).getParentFile();
        dataSet.write2Csv(new File(dir, "db-csv").getAbsolutePath());
        dataSet.write2Xml(new File(dir, "db.xml").getAbsolutePath());
    }
}
