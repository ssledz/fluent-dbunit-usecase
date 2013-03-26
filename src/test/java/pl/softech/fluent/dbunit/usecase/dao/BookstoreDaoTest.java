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
package pl.softech.fluent.dbunit.usecase.dao;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import javax.sql.DataSource;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import pl.softech.fluent.dbunit.usecase.Utils;
import pl.softech.fluent.dbunit.usecase.model.Author;
import pl.softech.fluent.dbunit.usecase.model.Book;
import pl.softech.fluentdbunit.DBExporter;
import pl.softech.fluentdbunit.DBLoader;
import static pl.softech.fluentdbunit.DBLoader.*;
import pl.softech.fluentdbunit.DBUtils;

/**
 *
 * @author Sławomir Śledź <slawomir.sledz@sof-tech.pl>
 */
public class BookstoreDaoTest {

    private static DataSource ds;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        ds = JdbcConnectionPool.create("jdbc:h2:mem:test", "sa", "sa");
        Utils.createTable(ds, "CREATE TABLE author (id identity, name varchar, lastname varchar)");
        Utils.createTable(ds, "CREATE TABLE book (id identity, title varchar, author_id int)");
    }

    @Test
    public void testFetchAuthorsByName() throws Exception {

        //loads data to author's table 
        DBLoader.forDB(ds).cleanInsert(dataSet(table("author", //
                cols("id", "name", "lastname"), //
                row("1", "Orson", "Card"), //
                row("2", "Terry", "Pratchett"), //
                row("3", "Magdalena", "Kozak"), //
                row("4", "Miroslav", "Žamboch"), //
                row("5", "Terry", "Goodkind") //
                )));

        BookstoreDao dao = new BookstoreDao(ds);
        Iterator<Author> it = dao.fetchAuthorsByName("Terry").iterator();
        Assert.assertEquals("Pratchett", it.next().getLastname());
        Assert.assertEquals("Goodkind", it.next().getLastname());

    }
    
    @Test
    public void testFetchAuthorsByName2() throws Exception {
        String dbxml = BookstoreDaoTest.class.getResource("db.xml").getFile();
        
        //loades data from file to memory
        IDataSet dataSet = DBLoader.fromXml(dbxml);
        
        //loads data to tables
        DBLoader.forDB(ds).cleanInsert(dataSet);
        
        //prints tables in dataset
        System.out.println(DBUtils.dataSet2String(dataSet));
        
        BookstoreDao dao = new BookstoreDao(ds);
        Iterator<Author> it = dao.fetchAuthorsByName("Terry").iterator();
        Assert.assertEquals("Pratchett", it.next().getLastname());
        Assert.assertEquals("Goodkind", it.next().getLastname());
    }
    
    @Test
    public void testSaveAuthor() throws Exception {

        //loads data to author's table 
        DBLoader.forDB(ds).cleanInsert(dataSet(table("author", //
                cols("id", "name", "lastname"), //
                row("5", "Orson", "Card") //
                )));

        BookstoreDao dao = new BookstoreDao(ds);
        Author[] authors = {
            new Author(-1, "Terry", "Pratchett"),
            new Author(5, "Orson Scott", "Card")
        };

        for (Author author : authors) {
            dao.save(author);
        }

        //compares two tables
        DBLoader.forDB(ds).assertQueryTable(table("author", //
                cols("name", "lastname"), //
                row("Orson Scott", "Card"), //
                row("Terry", "Pratchett")), //
                "SELECT name, lastname FROM author", "name");

    }

    @Test
    public void testFetchBooksByAuthor() throws Exception {

        //loads data to author's table 
        DBLoader.forDB(ds).cleanInsert(dataSet(table("author", //
                cols("id", "name", "lastname"), //
                row("5", "Orson", "Card") //
                )));
        //loads data to book's table 
        DBLoader.forDB(ds).cleanInsert(dataSet(table("book", //
                cols("id", "title", "author_id"), //
                row("1", "Ender's Game", "5") //
                )));

        //fetches whole author's table to memory
        ITable authorTable = DBExporter.forDB(ds).table("author").getDataSet().getTable("author");

        //prints table to system out
        System.out.println(DBUtils.dataSet2String(authorTable));

        //transforms whole author's table to collection of entities
        Collection<Author> authors = DBUtils.table2Entity(authorTable, new DBUtils.IEntityFactory<Author>() {
            public Collection<Author> create(DBUtils.ITableResultSet rset) {
                Collection<Author> authors = new LinkedList<Author>();
                while (rset.next()) {
                    authors.add(new Author(rset.getInteger("id"), rset.getString("name"), rset.getString("lastname")));
                }
                return authors;
            }
        });

        BookstoreDao dao = new BookstoreDao(ds);
        List<Book> books = dao.fetchBooksByAuthor(authors.iterator().next());
        Assert.assertEquals("Ender's Game", books.get(0).getTitle());

    }
}