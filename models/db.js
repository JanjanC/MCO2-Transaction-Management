const dotenv = require('dotenv');
const util = require('util');
const { Dao } = require('../models/dao');
const mysql = require('mysql');

dotenv.config();

const INTERVAL = 15000;

var nodeNum;
let pools = [];

async function monitorOutbox() {
    const query = `SELECT * FROM ${Dao.tables.outbox} WHERE ${Dao.outbox.status} = ?`;
    try {
        await dbs[nodeNum].startTransaction();
        let messages = await dbs[nodeNum].query(query, [Dao.MESSAGES.UNACKNOWLEDGED]);
        await dbs[nodeNum].commit();
    } catch {
        setTimeout(monitorOutbox, INTERVAL);
        return;
    }
    //console.log('data');
    for (let i of messages) {
        //console.log(i);
        await dbs[nodeNum].startTransaction();
        await dbs[i[Dao.outbox.recipient]]
            .insertInbox(i[Dao.outbox.id], nodeNum, i[Dao.outbox.message])
            .then(async () => {
                await dbs[i[Dao.outbox.recipient]].commit();
                await dbs[nodeNum].setMessageStatus(i[Dao.inbox.id], Dao.tables.outbox, Dao.MESSAGES.SENT);
                await dbs[nodeNum].commit();
            })
            .catch(async (error) => {
                await dbs[i[Dao.outbox.recipient]].rollback();
                await dbs[nodeNum].rollback();
                if (error.errno == 1062) {
                    // duplicate key entry
                    try {
                        await dbs[nodeNum].startTransaction();
                        await dbs[nodeNum].setMessageStatus(i[Dao.inbox.id], Dao.tables.outbox, Dao.MESSAGES.SENT);
                        await dbs[nodeNum].commit();
                    } catch {
                        if (!dbs[nodeNum].isDown) await dbs[nodeNum].rollback();
                    }
                }
                console.log('' + error);
            });
    }

    setTimeout(monitorOutbox, INTERVAL);
}

async function monitorInbox() {
    const query = `SELECT * 
                    FROM ${Dao.tables.inbox} 
                    WHERE ${Dao.inbox.status} =  ?
                    ORDER BY ${Dao.inbox.id} ASC`;

    try {
        await dbs[nodeNum].startTransaction();
        let messages = await dbs[nodeNum].query(query, [Dao.MESSAGES.UNACKNOWLEDGED]); //Retrieves the list of unacknowledged messages in the inbox
        console.log('MONITOR INBOX MESSAGES: ' + messages);
        await dbs[nodeNum].commit();
    } catch (error) {
        console.log('MONITOR INBOX: ' + error);
        setTimeout(monitorInbox, INTERVAL);
        return;
    }

    //Messages contains all queued up to go transactions
    //For each unacknowledged message
    for (let i of messages) {
        dbs[nodeNum]
            //Start a transaction in this current db
            .startTransaction()
            //If the transaction is successful...
            .then(async function () {
                await dbs[nodeNum].query(i[Dao.inbox.message]); //Executes the 'query' inside the inbox
                await dbs[nodeNum].setMessageStatus(i[Dao.inbox.id], Dao.tables.inbox, Dao.MESSAGES.ACKNOWLEDGED); //Set acknowledged to your own inbox
                await dbs[i[Dao.inbox.sender]].startTransaction(); //Starts the transaction for the other DB
                await dbs[i[Dao.inbox.sender]].setMessageStatus(i[Dao.outbox.id], Dao.tables.outbox, Dao.MESSAGES.ACKNOWLEDGED); //Set acknowledge to the outbox of the sender
                await Promise.all([dbs[nodeNum].commit(), dbs[i[Dao.inbox.sender]].commit()]); //Commit once it is queried and acknowledge ---> ENDS THE TRANSACTION
            })
            //Else if it failed
            .catch(function (error) {
                //throw error;
                console.log('catch monitor inbox ' + error);
                if (!dbs[nodeNum].isDown) dbs[nodeNum].rollback(); //Rollback if something went wrong AND the current nodeNum is not down
                //If the current nodeNum is down, it can be implied that the transaction is unsuccessful and an automatic rollback was executed
            });
    }

    setTimeout(monitorInbox, INTERVAL);
}

async function getConnections() {
    let dbs = {};
    for (let i = 1; i <= 3; i++) {
        dbs[i] = new Dao(i - 1);
        try {
            await dbs[i].initialize(pools[i - 1]);
        }
        catch (error) {
            console.log(error);
        }
    }
    return dbs;
}

function releaseConnections(dbs) {
    for (let i = 1; i <= 3; i++)
        if (dbs[i].connection) dbs[i].connection.release();
}

const db = {
    /*
    node - the current node number of the server
    */
    connect: async function (node) {
        nodeNum = node;
        for (let i = 0; i < 3; i++) pools.push(mysql.createPool(Dao.NODES[i]));

        //monitorOutbox();
        //monitorInbox();
    },

    // TODO: Configure the node and target the special scenario of either sending two messages, or sending to node 1 first
    insert: async function (name, year, rating, genre, director, actor_1, actor_2, callback) {
        let dbs = await getConnections();
        let params = [new Date().getTime(), name, year, rating, genre, director, actor_1, actor_2];
        let date = new Date().toISOString().slice(0, 19).replace('T', ' ');
        let query = '';
        let index = params[0];

        // Creates an array of promises for each node that contains a replica
        let nodesToInsert = [1, parseInt(year) < 1980 ? 2 : 3].map(function (value) {
            return dbs[value]
                .insert(...params)
                .then(function (result) {
                    query = dbs[value].lastSQLObject.sql;
                    return Promise.resolve(value);
                })
                .catch(function (error) {
                    return Promise.reject(value);
                });
        });

        let results = await Promise.allSettled(nodesToInsert);

        // Splits the results into the two arrays of node numbers depending on whether they failed
        // eg. failedSites = [0, 3]; workingSites = [1]
        let failedSites = [];
        let workingSites = [];
        for (i of results) {
            if (i.status == 'rejected') failedSites.push(i.reason);
            else workingSites.push(i.value);
        }

        console.log('failed sites: ' + failedSites);
        console.log('working sites: ' + workingSites);

        let inboxQuery = `INSERT INTO ${Dao.tables.outbox} (${Dao.outbox.id}, ${Dao.outbox.recipient}, ${Dao.outbox.message}, ${Dao.outbox.status})
                          VALUES(?, ?, ?, ?)`;

        // For each failed site, try to write to outbox, if unable rollback everything and send error
        for (let i of failedSites) {
            let messagesToSend = [];
            for (let j of workingSites) messagesToSend.push(dbs[j].query(inboxQuery, [date, i, query, Dao.MESSAGES.UNACKNOWLEDGED]));

            await Promise.any(messagesToSend).catch(function (error) {
                console.log(error.message);
                for (let j of workingSites) dbs[j].rollback();
                callback(0);
                return;
            });
        }

        // If able to write a message for all failed sites, commit transations
        try {
            for (let i of workingSites) dbs[i].commit();
            callback(index);
        } catch (error) {
            console.log(error);
            callback(0);
        }
    },

    find: async function (id, callback) {
        let dbs = await getConnections();
        let queryDb1 = () => {
            return dbs[1].find(id).then(async function (result) {
                await dbs[1].commit();
                console.log('call back in db1 query');
                callback(result);
            });
        };
        //Node 2/3 must check in both Node 2 and 3, because the year (and node #) cannot be determined through the id alone
        let queryDb2And3 = async () => {
            let nodesToFind = [2, 3].map(function (value) {
                return dbs[value].find(id).then(async function (result) {
                    console.log(result);
                    if (result == []) return Promise.reject('Did not find');
                    await dbs[value].commit(id);
                    return Promise.resolve(result);
                });
            });
            return Promise.any(nodesToFind).then(function (result) {
                console.log('call back in db2&3 query' + JSON.stringify(result));
                callback(result);
            });
        };

        if (nodeNum == 1) {
            queryDb1().catch(async function (error) {
                console.log('error ini db1 node num 1' + error);
                console.log('first catch 1');
                if (!dbs[1].isDown) await dbs[1].rollback();
                console.log('first catch 2');
                await queryDb2And3().catch(async function (error) {
                    console.log('error ini db1 node num 2' + error);
                    await Promise.all([dbs[2].rollback(), dbs[3].rollback()]);
                    callback(false);
                });
            });
        } else {
            queryDb2And3().catch(async function (error) {
                await Promise.all([dbs[2].rollback(), dbs[3].rollback()]);
                await queryDb1().catch(async function () {
                    await dbs[1].rollback();
                    callback(false);
                });
            });
        }
    },

    //TODO: ID NODE 2 SEARCH NODE 2 / 3 FIRST
    searchMovie: async function (name, callback) {
        let dbs = await getConnections();
        let queryDb1 = () => {
            return dbs[1].searchMovie(name).then(async function (result) {
                await dbs[1].commit();
                callback(result);
                return Promise.resolve('its fine');
            });
        };
        let queryDb2And3 = async () => {
            return Promise.all([dbs[2].searchMovie(name), dbs[3].searchMovie(name)]).then(async function (result) {
                await Promise.all([dbs[2].commit(), dbs[3].commit()]);
                callback(result[0].concat(result[1]));
                return Promise.resolve('its fine');
            });
        };

        if (nodeNum == 1) {
            queryDb1()
                .catch(async function () {
                    await dbs[1].rollback;
                    return queryDb2And3();
                })
                .catch(async function () {
                    await Promise.all([dbs[2].rollback(), dbs[3].rollback()]);
                    callback(false);
                });
        } else {
            queryDb2And3()
                .catch(async function () {
                    await Promise.all([dbs[2].rollback(), dbs[3].rollback()]);
                    return queryDb1();
                })
                .catch(async function () {
                    await dbs[1].rollback();
                    callback(false);
                });
        }
    },

    findAll: async function (callback) {
        let dbs = await getConnections();
        let queryDb1 = () => {
            return dbs[1].findAll().then(async function (result) {
                await dbs[1].commit();
                callback(result);
                return Promise.resolve('its fine');
            });
        };
        let queryDb2And3 = async () => {
            return Promise.all([dbs[2].findAll(), dbs[3].findAll()]).then(async function (result) {
                await Promise.all([dbs[2].commit(), dbs[3].commit()]); //Wait for both of the find to successfully commit
                callback(result[0].concat(result[1]));
                return Promise.resolve('its fine');
            });
        };

        if (nodeNum == 1) {
            queryDb1()
                .catch(async function () {
                    await dbs[1].rollback;
                    return queryDb2And3();
                })
                .catch(async function () {
                    await Promise.all([dbs[2].rollback(), dbs[3].rollback()]);
                    callback(false);
                });
        } else {
            queryDb2And3()
                .catch(async function () {
                    await Promise.all([dbs[2].rollback(), dbs[3].rollback()]);
                    return queryDb1();
                })
                .catch(async function () {
                    await dbs[1].rollback();
                    callback(false);
                });
        }
    },

    update: async function (id, name, year, rating, genre, director, actor_1, actor_2, callback) {
        let dbs = await getConnections();
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

        callback('INDEX 1: ' + index);

        let messagesToSend = [];
        for (let i of workingSites) {
            for (let j of failedSites) {
                messagesToSend.push(dbs[i].insertOutbox(date, j, query));
            }
        }

        await Promise.allSettled(messagesToSend);
    },

    delete: async function (id, callback) {
        let dbs = await getConnections();
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

        callback('INDEX 2: ' + index);

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
