const mysql = require('mysql');
const dotenv = require('dotenv');
const util = require('util');
const tc = require(`../models/tc.js`);

dotenv.config();

const NODE_1 = {
    host: 'db4free.net',
    port: 3306,
    user: 'g4_node_1',
    password: 'password',
    database: 'imdb_ijs_1',
};

const NODE_2 = {
    host: 'db4free.net',
    port: 3306,
    user: 'g4_node_2',
    password: 'password',
    database: 'imdb_ijs_2',
};

const NODE_3 = {
    host: 'db4free.net',
    port: 3306,
    user: 'g4_node_3',
    password: 'password',
    database: 'imdb_ijs_3',
};

var connection;
var promiseQuery;
var nodeNum;

const db = {
    status: true,
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

    connect: function (node, callback) {
        nodeNum = node;
        switch (node) {
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
        try {
            connection.connect();
            connection.query('SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;', function (error, result) {
                console.log('during');
                callback();
            });
            promiseQuery = util
                .promisify(connection.query)
                .bind(connection)
                .catch(function (error) {
                    // if error is because it couldnt connect
                    this.status = false;
                });
            this.status = true;
        } catch {
            this.status = false;
        }
    },

    xaTransaction: function (query, xid, data = {}) {
        let xaTransaction = `
            XA START '${xid}'
            ${query}
            XA END '${xid}'
        `;
        return promiseQuery(xaTransaction, data);
    },

    xaPrepare: function (xid) {
        return promiseQuery(`XA PREPARE '${xid}'`);
    },

    xaCommit: function (xid) {
        return promiseQuery(`XA COMMIT '${xid}'`);
    },

    xaRollback: function (xid) {
        return promiseQuery(`XA ROLLBACK '${xid}'`);
    },

    query: function (query, callback) {
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
    },

    insert: async function (name, year, rating, genre, director, actor_1, actor_2) {
        rating = rating ? rating : null;
        genre = genre ? genre : null;
        actor_1 = actor_1 ? actor_1 : null;
        actor_2 = actor_2 ? actor_2 : null;
        director = director ? director : null;
        const xid = new Date().toISOString();
        const query =
            'INSERT INTO movies (name, year, rating, genre, director, actor_1, actor_2) VALUES (?, ?, ?, ?, ?, ?, ?);';
        if (!this.status) this.connect();
        if ((this.status && nodeNum == 2 && req.body.year < 1980) || (nodeNum == 3 && req.body.year >= 1980)) {
            await this.xaTransaction(query, xid);
            await this.xaPrepare(xid);
        }
        //do something if theres an error here
        try {
            let commitRes = await Promise.all([
                tc.tryCommit(query, year, xid, [name, year, rating, genre, director, actor_1, actor_2]),
                this.xaCommit(xid),
            ]);
            return commitRes;
        } catch (e) {
            console.log(e);
            this.xaRollback;
            return e;
        }
    },

    find: async function (id) {
        const xid = new Date().toISOString();
        const query = 'SELECT * FROM movies WHERE id = ?';
        if (!this.status) this.connect();
        if (this.status) {
            await this.xaTransaction(query, xid);
            await this.xaPrepare(xid);
        }
        try {
            let [commitRes, localCommitRes] = await Promise.all([
                tc.tryCommit(query, null, xid, [id]),
                this.xaCommit(xid),
            ]);
            if (localCommitRes != []) return localCommitRes;
            for (let i of commitRes) if (i != []) return i;
        } catch (e) {
            console.log(e);
            this.xaRollback;
        }
    },

    findAll: async function (id) {
        const xid = new Date().toISOString();
        const query = 'SELECT * FROM movies';

        //Retrieve all movies
        if (nodeNum == 1) {
            if (this.status) {
                //Waits for a start transaction
                await this.xaTransaction(query, xid);
                //Waits for a prepare transaction
                await this.xaPrepare(xid);       
                //Returns a commit transaction         
                return await this.xaCommit(xid);
            } else {
                try {
                    // Returns the results of either xaRollback or xaCommit
                    // What SQL returns after a process is accomplished
                    let commitRes = await tc.tryCommit(query, null, xid, [id]);
                    let result = [];
                    
                    for (let i of commitRes) 
                        result.concat(i);                        
                    return result;
                } catch (e) {
                    console.log(e);
                    return [];
                }
            }
        }
        //Retrieve movies where year < 1980
        else if (nodeNum == 2) {
            if (this.status) {
                await this.xaTransaction(query, xid);
                await this.xaPrepare(xid);
                try {
                    let commitRes = await tc.tryCommit(query, null, xid, [id]);
                    let localRes = await this.xaCommit(xid);
                    let result = [];

                    for (let i of commitRes) 
                        result.concat(i);
                    return result;
                } catch (e) {
                    console.log(e);
                    this.xaRollback;
                    return [];
                }
            }
        }

        if ((nodeNum == 2 && req.body.year < 1980) || (nodeNum == 3 && req.body.year >= 1980)) {
            //EMPTY???
        }

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
                console.log(this.sql);
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

    update: function (id, name, year, rating, genre, director, actor_1, actor_2) {
        rating = rating ? rating : null;
        genre = genre ? genre : null;
        actor_1 = actor_1 ? actor_1 : null;
        actor_2 = actor_2 ? actor_2 : null;
        director = director ? director : null;
        const xid = new Date().toISOString();
        const query =
            'UPDATE movies SET name = ?, year = ?, rating = ?, genre = ?, director = ?, actor_1 = ?, actor_2 = ? WHERE id = ?';
        if ((nodeNum == 2 && req.body.year < 1980) || (nodeNum == 3 && req.body.year >= 1980)) {
            await this.xaTransaction(query, xid);
            await this.xaPrepare(xid);
        }
        //do something if theres an error here
        try {
            let commitRes = await tc.tryCommit(query, year, xid, [
                name,
                year,
                rating,
                genre,
                director,
                actor_1,
                actor_2,
            ]);
            await this.xaCommit;
            return commitRes;
        } catch (e) {
            console.log(e);
            this.xaRollback;
        }
    },

    delete: function (id) {
        const xid = new Date().toISOString();
        const query = 'DELETE FROM movies WHERE id = ?;';
        await this.xaTransaction(query, xid);
        await this.xaPrepare(xid);
        try {
            let commitRes = await tc.tryCommit(query, null, xid, [id]);
            await this.xaCommit;
            return commitRes;
        } catch (e) {
            console.log(e);
            this.xaRollback;
        }
    },
};

module.exports = db;
