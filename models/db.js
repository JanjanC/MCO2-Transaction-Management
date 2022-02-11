const dotenv = require('dotenv');
const { Dao } = require('../models/dao');

dotenv.config();

const SEND_INTERVAL = 5000;
const CHECK_INTERVAL = 1000;

var connection;
var dbs = {};
var nodeNum;

const db = {
    /*
    node - the current node number of the server
    */
    connect: function (node) {
        nodeNum = node;
        for (let i = 1; i <= Dao.NODES.length; i++)
            dbs[i] = new Dao(i - 1);
        console.log(dbs);
        setInterval(this.monitorOutbox, SEND_INTERVAL);
        setInterval(this.monitorInbox, CHECK_INTERVAL);
    },

    async monitorInbox() {
        const query = `SELECT * FROM ${Dao.tables.inbox} WHERE ${Dao.inbox.status} =  ?`;
        let messages = await dbs[nodeNum].query(query, [Dao.MESSAGES.UNACKNOWLEDGED]);

        const acknowledgeQuery = `
            UPDATE ${Dao.tables.outbox}
            SET ${Dao.outbox.status} = ?
        `;
        //Messages contains all queued up to go transactions
        for (let i in messages) {
            dbs[nodeNum].query(i[Dao.inbox.message])
            .then(function() {
                console.log(i)
                dbs[i[Dao.inbox.sender]].query(acknowledgeQuery, [Dao.MESSAGES.ACKNOWLEDGED]);
            })
        }
    },

    monitorOutbox: async function() {
        const query = `SELECT message FROM ${Dao.tables.outbox} WHERE ${Dao.inbox.status} = ?;`;
        let messages = await dbs[nodeNum].query(query, [Dao.MESSAGES.UNACKNOWLEDGED]);
        const insertQuery = `
            INSERT INTO ${Dao.tables.outbox}(${Dao.inbox.id}, ${Dao.inbox.message}, ${Dao.inbox.sender}, ${Dao.inbox.status})
            VALUES (?, ?, ?, ?))
        `;
        for (let i in messages) {
            dbs[i[Dao.outbox.recipient]]
            .query(insertQuery, [i[Dao.outbox.id], i[Dao.outbox.message], nodeNum]);
        }
    },

    insert: function (name, year, rating, genre, director, actor_1, actor_2) {
        if (year >= 1980) {
            if (JSON.stringify(dbs[nodeNum]) == JSON.stringify(otherDb[nodeNum])) {
                
            } else {

            }
        } else {
            
        }
    },

    find: function (id, callback) {
        
    },

    findAll: function (callback) {
        connection.beginTransaction(function (error) {
            if (error) throw error;

            const query = 'SELECT * FROM movies';
            connection.query(query, function (error, result) {
                if (error) {
                    return connection.rollback(function () {
                        throw error;
                    });
                }

                connection.commit(function (error) {
                    if (error) {
                        return connection.rollback(function () {
                            throw error;
                        });
                    }

                    return callback(result);
                });
            });
        });
    },

    searchMovie: function (name, callback) {
        connection.beginTransaction(function (error) {
            if (error) throw error;

            name = `%${name}%`;
            const query = 'SELECT * FROM movies WHERE name LIKE ?';
            connection.query(query, [name], function (error, result) {
                if (error) {
                    return connection.rollback(function () {
                        throw error;
                    });
                }
                connection.commit(function (error) {
                    if (error) {
                        return connection.rollback(function () {
                            throw error;
                        });
                    }

                    return callback(result);
                });
            });
        });
    },

    update: function (id, name, year, rating, genre, director, actor_1, actor_2, callback) {
        rating = rating ? rating : null;
        genre = genre ? genre : null;
        actor_1 = actor_1 ? actor_1 : null;
        actor_2 = actor_2 ? actor_2 : null;
        director = director ? director : null;

        connection.beginTransaction(function (error) {
            if (error) throw error;

            const query =
                'UPDATE movies SET name = ?, year = ?, rating = ?, genre = ?, director = ?, actor_1 = ?, actor_2 = ? WHERE id = ?';
            connection.query(
                query,
                [name, year, rating, genre, director, actor_1, actor_2, id],
                function (error, result) {
                    if (error) {
                        return connection.rollback(function () {
                            throw error;
                        });
                    }

                    connection.commit(function (error) {
                        if (error) {
                            return connection.rollback(function () {
                                throw error;
                            });
                        }

                        return callback(result);
                    });
                }
            );
        });
    },

    delete: function (id, callback) {
        connection.beginTransaction(function (error) {
            if (error) throw error;

            const query = 'DELETE FROM movies WHERE id = ?;';
            connection.query(query, [id], function (error, result) {
                if (error) {
                    return connection.rollback(function () {
                        throw error;
                    });
                }

                connection.commit(function (error) {
                    if (error) {
                        return connection.rollback(function () {
                            throw error;
                        });
                    }

                    return callback(result);
                });
            });
        });
    },
};

module.exports = db;