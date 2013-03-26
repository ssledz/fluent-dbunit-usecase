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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import javax.sql.DataSource;
import pl.softech.fluent.dbunit.usecase.model.*;

/**
 *
 * @author Sławomir Śledź <slawomir.sledz@sof-tech.pl>
 */
public class BookstoreDao {

    private final DataSource ds;

    public BookstoreDao(DataSource ds) {
        this.ds = ds;
    }

    public List<Author> fetchAuthorsByName(String name) throws SQLException {

        Connection conn = null;
        List<Author> authors = new LinkedList<Author>();

        try {
            conn = ds.getConnection();

            PreparedStatement pst = conn.prepareStatement("SELECT * FROM author where name = ?");
            pst.setString(1, name);
            ResultSet rs = pst.executeQuery();
            while (rs.next()) {
                authors.add(new Author(rs.getLong("id"), rs.getString("name"), rs.getString("lastname")));
            }

            return authors;

        } finally {
            if (conn != null) {
                conn.close();
            }
        }

    }

    public void save(Author author) throws SQLException {

        Connection conn = null;
        try {
            conn = ds.getConnection();
            Object[] args = {
                author.getName(), author.getLastname(), author.getId()
            };
            
            PreparedStatement pst = conn.prepareStatement("UPDATE author set name = ?, lastname = ? where id = ?");
            int index = 1;
            for(Object arg : args) {
                pst.setObject(index++, arg);
            }

            if(pst.executeUpdate() == 0) {
                
                pst = conn.prepareStatement("INSERT INTO author (name, lastname) VALUES (?,?)");
                for(index = 0; index < args.length - 1; index++) {
                    pst.setObject(index + 1, args[index]);
                }
                pst.executeUpdate();
                ResultSet rs = pst.getGeneratedKeys();
                if(rs.next()) {
                    author.setId(rs.getInt(1));
                }
            }
            

        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    public List<Book> fetchBooksByAuthor(Author author) throws SQLException {

        Connection conn = null;
        List<Book> books = new LinkedList<Book>();

        try {
            conn = ds.getConnection();

            PreparedStatement pst = conn.prepareStatement("SELECT * FROM book where author_id = ?");
            pst.setLong(1, author.getId());
            ResultSet rs = pst.executeQuery();
            while (rs.next()) {
                books.add(new Book(rs.getLong("id"), rs.getString("title"), author));
            }

            return books;

        } finally {
            if (conn != null) {
                conn.close();
            }
        }

    }
}
