const mysql = require('mysql');
const util = require('util');

class Dao {
    static NODES = [
        {
            host: 'db4free.net',
            port: 3306,
            user: 'g4_node_1',
            password: 'password',
            database: 'imdb_ijs_1',
            connectTimeout: 5000,
        },
        {
            host: 'db4free.net',
            port: 3306,
            user: 'g4_node_2',
            password: 'password',
            database: 'imdb_ijs_2',
            connectTimeout: 5000,
        },
        {
            host: 'db4free.net',
            port: 3306,
            user: 'g4_node_3',
            password: 'password',
            database: 'imdb_ijs_3',
            connectTimeout: 5000,
        },
    ];
    static MESSAGES = {
        ACKNOWLEDGED: 'ACKNOWLEDGED',
        UNACKNOWLEDGED: 'UNACKNOWLEDGED',
        SENT: 'SENT',
        CONNECTED: 'CONNECTED',
        UNCONNECTED: 'UNCONNECTED',
    };
    static tables = {
        imdb: 'movies',
        inbox: 'messages_received',
        outbox: 'messages_to_send',
    };
    static imdb = {
        id: 'id',
        name: 'name',
        year: 'year',
        rating: 'rating',
        genre: 'genre',
        director: 'director',
        actor_1: 'actor_1',
        actor_2: 'actor_2',
    };
    static inbox = {
        id: 'id',
        sender: 'sender',
        message: 'message',
        status: 'message_status', // default unacknowledged
    };
    static outbox = {
        id: 'id',
        recipient: 'recipient',
        message: 'message',
        status: 'message_status', // default unacknowledged
    };

    static CHECK_INTERVAL = 2000;
    static SEND_INTERVAL = 2000;

    connection;
    isDown;
    query;
    queryString;
    node;
    lastSQLObject;

    constructor(node) {
        this.node = node;
    }

    initialize() {
        if (this.connection) this.connection.destroy();
        this.connection = mysql.createConnection(Dao.NODES[this.node]);

        /**
         *
         * @param {String} query
         * @param {any[]} options
         * @returns Promise of
         */
        let promiseQuery = (query, options) => {
            console.log('query: ' + query);
            console.log('options: ' + options);
            return new Promise((resolve, reject) => {
                this.lastSQLObject = this.connection.query(query, options, function (error, results) {
                    if (error) reject(error);
                    else resolve(results);
                });
            }).catch((error) => {
                console.log(error.message);
                //if (error.message.split(' ')[1] == 'ENOTFOUND' || error.message.split(' ')[1] == 'ETIMEDOUT')
                this.isDown = true;
                return error;
            });
        };
        return new Promise((resolve, reject) => {
            this.connection.connect((error) => {
                if (error) {
                    this.query = function (query, options) {
                        return this.initialize().then((value) => {
                            return promiseQuery;
                        });
                    };
                    this.isDown = true;
                    reject(Dao.MESSAGES.UNCONNECTED);
                } else {
                    this.query = promiseQuery;
                    this.isDown = false;
                    resolve(Dao.MESSAGES.CONNECTED);
                }
            });
        });
    }

    insert(id, name, year, rating, genre, director, actor_1, actor_2) {
        rating = rating ? rating : null;
        genre = genre ? genre : null;
        actor_1 = actor_1 ? actor_1 : null;
        actor_2 = actor_2 ? actor_2 : null;
        director = director ? director : null;

        return this.query('START TRANSACTION;').then((result) => {
            console.log('got here');
            return this.query(
                `
                INSERT INTO ${Dao.tables.imdb} (${Dao.imdb.id}, ${Dao.imdb.name}, ${Dao.imdb.year}, ${Dao.imdb.rating}, ${Dao.imdb.genre}, ${Dao.imdb.director}, ${Dao.imdb.actor_1}, ${Dao.imdb.actor_2})
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            `,
                [id, name, year, rating, genre, director, actor_1, actor_2]
            );
        });
    }

    update(id, name, year, rating, genre, director, actor_1, actor_2) {
        rating = rating ? rating : null;
        genre = genre ? genre : null;
        actor_1 = actor_1 ? actor_1 : null;
        actor_2 = actor_2 ? actor_2 : null;
        director = director ? director : null;

        return this.query('START TRANSACTION;').then((result) => {
            return this.query(
                `
                UPDATE ${Dao.tables.imdb} 
                SET ${Dao.imdb.name} = ?, ${Dao.imdb.year} = ?, ${Dao.imdb.rating} = ?, ${Dao.imdb.genre} = ?, ${Dao.imdb.director} = ?, ${Dao.imdb.actor_1} = ?, ${Dao.imdb.actor_2} = ?
                WHERE ${Dao.imdb.id} = ?
            `,
                [name, year, rating, genre, director, actor_1, actor_2, id]
            );
        });
    }

    delete(id) {
        return this.query('START TRANSACTION;').then((result) => {
            return this.query(
                `
                DELETE FROM ${Dao.tables.imdb}
                WHERE ${Dao.imdb.id} = ?
            `,
                [id]
            );
        });
    }

    find(id) {
        return this.query('START TRANSACTION;').then((result) => {
            return this.query(
                `
                SELECT * FROM ${Dao.tables.imdb}
                WHERE ${Dao.imdb.id} = ?
            `,
                [id]
            );
        });
    }

    findAll() {
        return this.query('START TRANSACTION;').then((result) => {
            return this.query(`SELECT * FROM ${Dao.tables.imdb};`);
        });

        //return this.query(`SELECT * FROM ${Dao.tables.imdb};`)
    }

    searchMovie(name) {
        return this.query('START TRANSACTION;').then((result) => {
            return this.query(`SELECT * FROM ${Dao.tables.imdb} WHERE name LIKE ?`, [`%${name}%`]);
        });
    }

    commit() {
        return this.query('COMMIT;');
        //return this.connection.commit();
    }

    rollback() {
        return this.query('ROLLBACK;');
    }

    insertOutbox(date, recipient, query) {      
        return this.query('START TRANSACTION;').then((result) => {
            return this.query(
                `
                INSERT INTO ${Dao.tables.outbox} (${Dao.outbox.id}, ${Dao.outbox.recipient}, ${Dao.outbox.message}, ${Dao.outbox.status})
                VALUES(?, ?, ?, ?)
                `,
                [date, recipient, query, Dao.MESSAGES.UNACKNOWLEDGED]
            );
        });
    }

    insertInbox(date, sender, query) {
        return this.query('START TRANSACTION;').then((result) => {
            return this.query(
                `
                INSERT INTO ${Dao.tables.inbox} (${Dao.inbox.id}, ${Dao.inbox.sender}, ${Dao.inbox.message}, ${Dao.inbox.status})
                VALUES(?, ?, ?, ?)
            `,
                [date, sender, query, Dao.MESSAGES.UNACKNOWLEDGED]
            );
        });
    }

    setMessageStatus(id, status) {
        return this.query('START TRANSACTION;').then((result) => {
            return this.query(
                `
                UPDATE ${Dao.tables.outbox}
                SET ${Dao.outbox.status} = ?
                WHERE ${Dao.outbox.id} = ?
            `,
                [status, id]
            );
        });
    }
}

module.exports = { Dao };
