const dotenv = require('dotenv');
const util = require('util');
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
    connect: async function (node) {
        nodeNum = node;
        let inits = [];
        for (let i = 1; i <= Dao.NODES.length; i++) {
            dbs[i] = new Dao(i - 1);
            inits.push(dbs[i].initialize());
        }
        await Promise.allSettled(inits);
        //setInterval(this.monitorOutbox, SEND_INTERVAL);
        //setInterval(this.monitorInbox, CHECK_INTERVAL);
    },

    monitorInbox: async function () {
        const query = `SELECT * FROM ${Dao.tables.inbox} WHERE ${Dao.inbox.status} =  ?`; //order by?
        let messages = await dbs[nodeNum].query(query, [Dao.MESSAGES.UNACKNOWLEDGED]);
        //Messages contains all queued up to go transactions
        for (let i in messages) {
            dbs[nodeNum]
                .query(i[Dao.inbox.message])
                .then(function () {
                    return dbs[nodeNum].commit();
                })
                .then(async function () {
                    await dbs[i[Dao.inbox.sender]].setMessageStatus(Dao.MESSAGES.ACKNOWLEDGED, i[i.inbox.id]); // i think
                    dbs[nodeNum].setMessageStatus(Dao.MESSAGES.ACKNOWLEDGED, i[i.inbox.id]);
                })
                .catch(function (error) {
                    console.log(error);
                });
        }
    },

    //send to a message to other nodes
    monitorOutbox: async function () {
        let query = `SELECT * FROM ${Dao.tables.outbox} WHERE ${Dao.outbox.status} = ?;`;
        let messages = await dbs[nodeNum].query(query, [Dao.MESSAGES.UNACKNOWLEDGED]);
        //console.log('data');
        for (let i of messages) {
            //console.log(i);
            await dbs[i[Dao.outbox.recipient]]
                .insertInbox(i[Dao.outbox.id], nodeNum, i[Dao.outbox.message])
                .then(async () => {
                    await dbs[i[Dao.outbox.recipient]].commit();
                    await dbs[nodeNum].setMessageStatus(Dao.MESSAGES.SENT, i[i.inbox.id]);
                    await dbs[nodeNum].commit();
                })
                .catch(async (error) => {
                    await dbs[i[Dao.outbox.recipient]].rollback();
                    await dbs[nodeNum].rollback();
                    if (error.errno == 1062) {
                        // duplicate key entry
                        await dbs[nodeNum].setMessageStatus(Dao.MESSAGES.SENT, i[i.inbox.id]);
                        await dbs[nodeNum].commit();
                    }
                    console.log(error.name);
                });
        }
    },

    // TODO: Configure the node and target the special scenario of either sending two messages, or sending to node 1 first
    insert: async function (name, year, rating, genre, director, actor_1, actor_2, callback) {
        let params = [new Date().getTime(), name, year, rating, genre, director, actor_1, actor_2];
        let date = new Date().toISOString().slice(0, 19).replace('T', ' ');
        let query = '';
        let index;

        // Creates an array of promises for each node that contains a replica
        let nodesToInsert = [1, parseInt(year) < 1980 ? 2 : 3].map(function (value) {
            return dbs[value]
                .insert(...params)
                .then(function (result) {
                    query = dbs[value].lastSQLObject.sql;
                    index = result;
                    return Promise.resolve(value);
                })
                .catch(function () {
                    query = dbs[value].lastSQLObject.sql;
                    index = 0;
                    return Promise.reject(value);
                });
        });

        let results = await Promise.allSettled(nodesToInsert);
        callback(index);

        let inboxQuery = `
            INSERT INTO ${Dao.tables.outbox} (${Dao.outbox.id}, ${Dao.outbox.recipient}, ${Dao.outbox.message}, ${Dao.outbox.status})
            VALUES(?, ?, ?, ?)
        `;

        // Splits the results into the two arrays of node numbers depending on whether they failed
        // eg. failedSites = [0, 3]; workingSites = [1]
        let [failedSites, workingSites] = results.reduce(
            function ([failed, working], result) {
                return result.status == 'rejected'
                    ? [[...failed, result.value], working]
                    : [[failed], [...working, result.value]];
            },
            [[], []]
        );

        // For each failed site, try to write to outbox, if unable rollback everything and send error
        for (let i of failedSites) {
            let messagesToSend = [];
            for (let j of workingSites) messagesToSend.push(dbs[j].query(inboxQuery, [date, i, query]));
            await Promise.any(messagesToSend).catch(function () {
                for (let j of workingSites) {
                    dbs[j].rollback();
                    callback(false);
                    return;
                }
            });
        }
        // If able to write a message for all failed sites, commit transations
        for (let i of workingSites) dbs[i].commit();
        callback(index);
    },

    find: async function (id, callback) {
        let queryDb1 = () => {
            return dbs[1].find(id).then(async function (result) {
                await dbs[1].commit();
                callback(result);
            });
        };
        let queryDb2And3 = async () => {
            await Promise.all([dbs[2].find(id), dbs[3].find(id)]);
            return Promise.all([dbs[2].commit(), dbs[3].commit()]);
        };
        let allServersDown = async () => {
            await Promise.all([dbs[1].rollback(), dbs[2].rollback(), dbs[3].rollback()]);
            callback(false);
        };

        if (nodeNum == 1) {
            queryDb1().catch(queryDb2And3).catch(allServersDown);
        } else {
            queryDb2And3().catch(queryDb1).catch(allServersDown);
        }
    },

    // TODO ID NODE 2 SEARCH NODE 2 / 3 FIRST
    searchMovie: async function (name, callback) {
        let queryDb1 = () => {
            return dbs[1].searchMovie(name).then(async function (result) {
                await dbs[1].commit();
                callback(result);
            });
        };
        let queryDb2And3 = async () => {
            await Promise.all([dbs[2].searchMovie(name), dbs[3].searchMovie(name)]);
            return Promise.all([dbs[2].commit(), dbs[3].commit()]);
        };
        let allServersDown = async () => {
            await Promise.all([dbs[2].rollback(), dbs[3].rollback()]);
            callback(false);
        };

        if (nodeNum == 1) {
            queryDb1().catch(queryDb2And3).catch(allServersDown);
        } else {
            queryDb2And3().catch(queryDb1).catch(allServersDown);
        }
    },

    findAll: function (callback) {
        dbs[1]
            .findAll()
            .then(async function (result) {
                await dbs[1].commit();
                callback(result);
            })
            .catch(async function (error) {
                dbs[1].rollback();
                if (error == Dao.MESSAGES.UNCONNECTED) {
                    let results = await Promise.all([dbs[2].findAll(), dbs[3].findAll()]);
                    await Promise.all([dbs[2].commit(), dbs[3].commit()]);
                    callback(result[0].concat(result[1]));
                }
            })
            .catch(async function (error) {
                //all servers are down :/
                await Promise.all([dbs[2].rollback(), dbs[3].rollback()]);
                callback(false);
            });
    },

    update: async function (id, name, year, rating, genre, director, actor_1, actor_2, callback) {
        let params = [id, name, year, rating, genre, director, actor_1, actor_2];
        let date = new Date().toISOString().slice(0, 19).replace('T', ' ');
        let query = '';
        let index;

        // Creates an array of promises for each node that contains a replica
        let nodesToUpdate = [1, parseInt(year) < 1980 ? 2 : 3].map(function (value) {
            return dbs[value]
                .update(...params)
                .then(function (result) {
                    query = dbs[value].lastSQLObject.sql;
                    index = result;
                    dbs[value].commit();
                    return Promise.resolve(value);
                })
                .catch(function () {
                    dbs[value].rollback();
                    query = dbs[value].lastSQLObject.sql;
                    index = 0;
                    return Promise.reject(value);
                });
        });

        let results = await Promise.allSettled(nodesToUpdate);
        let failedSites = [];
        let workingSites = [];
        for (let i of results) {
            if ((i.status = 'rejected')) {
                failedSites.push(i.value);
            } else {
                workingSites.push(i.value);
            }
        }

        callback(index);

        let messagesToSend = [];
        for (let i of workingSites) {
            for (let j of failedSites) {
                messagesToSend.push(dbs[i].insertOutbox(date, j, query));
            }
        }

        await Promise.allSettled(messagesToSend);
    },

    delete: async function (id, callback) {
        let date = new Date().toISOString().slice(0, 19).replace('T', ' ');
        let query = '';
        let index;

        // Creates an array of promises for each node that contains a replica
        let nodesToDelete = [1, 2, 3].map(function (value) {
            return dbs[value]
                .delete(id)
                .then(function (result) {
                    query = dbs[value].lastSQLObject.sql;
                    index = result;
                    dbs[value].commit();
                    return Promise.resolve(value);
                })
                .catch(function () {
                    dbs[value].rollback();
                    query = dbs[value].lastSQLObject.sql;
                    index = 0;
                    return Promise.reject(value);
                });
        });

        let results = await Promise.allSettled(nodesToDelete);
        let failedSites = [];
        let workingSites = [];
        for (let i of results) {
            if ((i.status = 'rejected')) {
                failedSites.push(i.value);
            } else {
                workingSites.push(i.value);
            }
        }

        callback(index);

        let messagesToSend = [];
        for (let i of workingSites) {
            for (let j of failedSites) {
                messagesToSend.push(dbs[i].insertOutbox(date, j, query));
            }
        }

        await Promise.allSettled(messagesToSend);
    },
};

module.exports = db;
