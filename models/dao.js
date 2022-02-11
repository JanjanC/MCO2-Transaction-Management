const mysql = require('mysql');
const util = require('util');

class Dao {
    static NODES = [
        {
            host: 'google.com',
            port: 3306,
            user: 'g4_node_1',
            password: 'password',
            database: 'imdb_ijs_1',
        },
        {
            host: 'google.com',
            port: 3306,
            user: 'g4_node_2',
            password: 'password',
            database: 'imdb_ijs_2',
        },
        {
            host: 'google.com',
            port: 3306,
            user: 'g4_node_3',
            password: 'password',
            database: 'imdb_ijs_3',
        }
    ];
    static MESSAGES = {
        ACKNOWLEDGED: 'ACKNOWLEDGED',
        UNACKNOWLEDGED: 'UNACKNOWLEDGED',
    };
    static tables = {
        imdb: "IMDB_IJS",
        inbox: "messages_received",
        outbox: "messages_to_send"
    };
    static imdb = {
        id: 'id',
        name: 'name',
        year: 'year',
        rating: 'rating',
        genre: 'genre',
        director: 'director',
        actor_1: 'actor_1',
        actor_2: 'actor_2'
    };
    static inbox = {
        id: 'id',
        sender: 'sender',
        message: 'message',
        status: 'message_status'    // default unacknowledged
    };
    static outbox = {
        id: 'id',
        recipient: 'recipient',
        message: 'message',
        status: 'message_status'    // default unacknowledged
    };
    
    static CHECK_INTERVAL = 2000;
    static SEND_INTERVAL = 2000;
    connection;
    isDown;
    query;
    node;

    constructor(node) {
        try {
            this.node = node;
            this.connection = mysql.createConnection(Dao.NODES[node]);
            this.connection.connect();
        // this.query = util.promisify(this.connection.query).bind(this.connection);
            this.query = function (query, options) {
                return new Promise(function(resolve, reject) {
                    this.connection.query(query, options, function(error, results) {
                        if (error)
                            reject(error);
                        else
                            resolve(results);
                    })
                }.bind(this))
                .catch(function(error) {
                    console.log(error.message)
                    if (error.message.split(' ')[1] == 'ENOTFOUND' ||
                    error.message.split(' ')[1] == 'ETIMEDOUT')
                        this.isDown = true;
                    return error;
                }.bind(this));
            };
            this.query('SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;');
            this.isDown = false;
        }
        catch(error) {
            console.log(error.message);
            this.query = function(query, values) {}
            if (error && (error.message.split(' ')[1] == 'ENOTFOUND' ||
            error.message.split(' ')[1] == 'ETIMEDOUT'))
                this.isDown = true;
        }
    };

    insert(id, name, year, rating, genre, director, actor_1, actor_2) {
        rating = rating ? rating : null;
        genre = genre ? genre : null;
        actor_1 = actor_1 ? actor_1 : null;
        actor_2 = actor_2 ? actor_2 : null;
        director = director ? director : null;

        return this.query(`
            START TRANSACTION
            INSERT INTO ${tables.imdb}(${imdb.id}, ${imdb.name}, ${imdb.year}, ${imdb.rating}, ${imdb.genre}, ${imdb.director}, ${imdb.actor_1}, ${imdb.actor_2})
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            END TRANSACTION
        `, [id, name, year, rating, genre, director, actor_1, actor_2])
    };

    update(id, name, year, rating, genre, director, actor_1, actor_2) {
        rating = rating ? rating : null;
        genre = genre ? genre : null;
        actor_1 = actor_1 ? actor_1 : null;
        actor_2 = actor_2 ? actor_2 : null;
        director = director ? director : null;
        
        return this.query(`
            START TRANSACTION
            UPDATE ${tables.imdb} 
            SET ${imdb.name} = ?, ${imdb.year} = ?, ${imdb.rating} = ?, ${imdb.genre} = ?, ${imdb.director} = ?, ${imdb.actor_1} = ?, ${imdb.actor_2} = ?
            WHERE ${imdb.id} = ?
            END TRANSACTION
        `, [name, year, rating, genre, director, actor_1, actor_2, id])
    };

    delete(id) {
        return this.query(`
            START TRANSACTION
            DELETE FROM ${tables.imdb}
            WHERE ${imdb.id} = ?
            END TRANSACTION
        `, [id])
    };

    find(id) {
        return this.query(`
            START TRANSACTION
            SELECT * FROM ${tables.imdb}
            WHERE ${imdb.id} = ?
            END TRANSACTION
        `, [id])
    }

    searchMovie (name) {
        const query = `SELECT * FROM ${tables.imdb} WHERE name LIKE ?`;
        return this.query(`
            START TRANSACTION
            SELECT * FROM ${tables.imdb}
            WHERE ${imdb.name} = ?
            END TRANSACTION
        `, [`%${name}%`])
    };

    commit() {
        return this.connection.commit();
    };

    rollback() {
        return this.connection.rollback();
    };    
}

module.exports = {Dao};