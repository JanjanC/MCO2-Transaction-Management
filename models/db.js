const mysql = require('mysql');
const dotenv = require('dotenv');

dotenv.config();

if (process.env.NODE == 1) {
    hostname = process.env.DB_HOSTNAME_1;
    port = process.env.DB_PORT_1;
    username = process.env.DB_USERNAME_1;
    password = process.env.DB_PASSWORD_1;
    dbname = process.env.DB_DBNAME_1;
} else if (process.env.NODE == 2) {
    hostname = process.env.DB_HOSTNAME_2;
    port = process.env.DB_PORT_2;
    username = process.env.DB_USERNAME_2;
    password = process.env.DB_PASSWORD_2;
    dbname = process.env.DB_DBNAME_2;
} else if (process.env.NODE == 3) {
    hostname = process.env.DB_HOSTNAME_3;
    port = process.env.DB_PORT_3;
    username = process.env.DB_USERNAME_3;
    password = process.env.DB_PASSWORD_3;
    dbname = process.env.DB_DBNAME_3;
}

var connection = mysql.createConnection({
    host: hostname,
    port: port,
    user: username,
    password: password,
    database: dbname,
});

const db = {
    connect: function () {
        connection.connect();
    },
    query: function (query, callback) {
        connection.query(query, function (error, result) {
            if (error) throw error;
            return callback(result);
        });
    },
};

module.exports = db;
