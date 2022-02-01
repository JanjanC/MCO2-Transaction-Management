const mysql = require('mysql');
const dotenv = require('dotenv');

dotenv.config();

const NODE_1 = {
    host: process.env.DB_HOSTNAME_1,
    port: process.env.DB_PORT_1,
    user: process.env.DB_USERNAME_1,
    password: process.env.DB_PASSWORD_1,
    database: process.env.DB_DBNAME_1,
};

const NODE_2 = {
    host: process.env.DB_HOSTNAME_2,
    port: process.env.DB_PORT_2,
    user: process.env.DB_USERNAME_2,
    password: process.env.DB_PASSWORD_2,
    database: process.env.DB_DBNAME_2,
};

const NODE_3 = {
    host: process.env.DB_HOSTNAME_3,
    port: process.env.DB_PORT_3,
    user: process.env.DB_USERNAME_3,
    password: process.env.DB_PASSWORD_3,
    database: process.env.DB_DBNAME_3,
};

var connection;

switch (process.env.NODE) {
    case '1':
        connection = mysql.createConnection(NODE_1);
        break;
    case '2':
        connection = mysql.createConnection(NODE_2);
        break;
    case '3':
        connection = mysql.createConnection(NODE_3);
        break;
}

const db = {
    columns: {
        id: 'id',
        name: 'name',
        year: 'year',
        rating: 'rating',
        genre: 'genre',
        director: 'director',
        actor_1: 'actor_1',
        actor_2: 'actor_2',
    },

    connect: function () {
        connection.connect();
    },

    query: function (query, callback) {
        connection.query(query, function (error, result) {
            if (error) throw error;
            return callback(result);
        });
    },

    insert: function (name, year, rating, genre, director, actor_1, actor_2, callback) {
        rating = rating ? rating : 'NULL';
        genre = genre ? `'${genre}'` : 'NULL';
        actor_1 = actor_1 ? `'${actor_1}'` : 'NULL';
        actor_2 = actor_2 ? `'${actor_2}'` : 'NULL';
        director = director ? `'${director}'` : 'NULL';

        const query = `INSERT INTO movies (name, year, rating, genre, director, actor_1, actor_2) VALUES ('${name}', ${year}, ${rating}, ${genre}, ${director}, ${actor_1}, ${actor_2});`;
        connection.query(query, function (error, result) {
            if (error) throw error;
            console.log('INSERT RESULT IS: ' + JSON.stringify(result));
            return callback(result);
        });
    },

    find: function (projection, conditions, callback) {
        const query = `
            SELECT ${projection.reduce(function (string, curr) {
                if (string != '') string += ' ';
                return string + curr;
            }, '')}
            FROM movies
            ${conditions.reduce(function (string, curr) {
                if (string != '') string += ' AND\n';
                else string += 'WHERE ';
                return string + curr;
            }, '')};
        `;
        connection.query(query, function (error, result) {
            if (error) throw error;
            return callback(result);
        });
    },

    update: function (values, conditions, callback) {
        for (let i in values) if (!values[i]) delete values[i];
        const query = `
            UPDATE movies
            SET ${Object.keys(values).reduce(function (string, key) {
                if (string != '') string += ', ';
                if (typeof values[key] == 'string') values[key] = `'${values[key]}'`;
                return string + key + ' = ' + values[key];
            }, '')}
            ${conditions.reduce(function (string, curr) {
                if (string != '') string += ' AND\n';
                else string += 'WHERE ';
                return string + curr;
            }, '')};
        `;
        connection.query(query, function (error, result) {
            if (error) throw error;
            return callback(result);
        });
    },

    delete: function (id, callback) {
        const query = `
            DELETE FROM movies 
            WHERE id = ${id};
        `;
        connection.query(query, function (error, result) {
            if (error) throw error;
            return callback(result);
        });
    },
};

module.exports = db;
