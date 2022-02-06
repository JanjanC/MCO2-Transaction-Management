const mysql = require('mysql');
const dotenv = require('dotenv');

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

    connect: function (node, callback) {
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
        connection.connect();
        connection.query('SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;', function (error, result) {
            console.log('during');
            callback();
        });
    },

    query: function (query, callback) {
        connection.beginTransaction(function (error) {
            if (error) throw error;

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

    insert: function (name, year, rating, genre, director, actor_1, actor_2, callback) {
        rating = rating ? rating : null;
        genre = genre ? genre : null;
        actor_1 = actor_1 ? actor_1 : null;
        actor_2 = actor_2 ? actor_2 : null;
        director = director ? director : null;

        connection.beginTransaction(function (error) {
            if (error) throw error;

            const query =
                'INSERT INTO movies (name, year, rating, genre, director, actor_1, actor_2) VALUES (?, ?, ?, ?, ?, ?, ?);';

            connection.query(query, [name, year, rating, genre, director, actor_1, actor_2], function (error, result) {
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

                    console.log('INSERT RESULT IS: ' + JSON.stringify(result));
                    return callback(result);
                });
            });
        });
    },

    find: function (id, callback) {
        connection.beginTransaction(function (error) {
            if (error) throw error;

            const query = 'SELECT * FROM movies WHERE id = ?';
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
