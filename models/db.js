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
        const query = `SELECT * FROM ${Dao.tables.inbox} WHERE ${Dao.inbox.status} =  ?`;
        let messages = await dbs[nodeNum].query(query, [Dao.MESSAGES.UNACKNOWLEDGED]);
        //Messages contains all queued up to go transactions
        for (let i in messages) {
            dbs[nodeNum]
                .query(i[Dao.inbox.message])
                .then(function () {
                    return dbs[nodeNum].commit();
                })
                .then(async function () {
                    await dbs[i[Dao.inbox.sender]].setMessageStatus(Dao.MESSAGES.ACKNOWLEDGED, i[i.inbox.id]);
                    dbs[nodeNum].setMessageStatus(Dao.MESSAGES.ACKNOWLEDGED, i[i.inbox.id]);
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
                    console.log(error.name);
                });
        }
    },

    // TODO: Configure the node and target the special scenario of either sending two messages, or sending to node 1 first
    insert: async function (name, year, rating, genre, director, actor_1, actor_2, callback) {
        let params = [new Date().getTime(), name, year, rating, genre, director, actor_1, actor_2];
        let date = new Date().toISOString().slice(0, 19).replace('T', ' ');
        //let otherNode = nodeNum == 1 ? (parseInt(year) < 1980 ? 2 : 3) : 1;
        let query = '';
        let index;

        // Creates an array of promises for each node that contains a replica
        let nodesToInsert = [1, parseInt(year) < 1980 ? 2 : 3].map(function (value) {
            return dbs[value]
                .insert(...params)
                .then(function (result) {
                    query = dbs[value].lastSQLObject.sql;
                    index = params[0];
                    dbs[value].commit();

                    return Promise.resolve(value);
                })
                .catch(function () {
                    dbs[value].rollback();
                    index = 0;
                    callback(index);
                    return Promise.reject(value);
                });
        });

        let results = await Promise.allSettled(nodesToInsert);
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

        let messageResults = await Promise.allSettled(messagesToSend);

        // let insertOthers = () => {
        //     return dbs[otherNode]
        //         .insert(...params)
        //         .then(async function (result) {
        //             let query = dbs[otherNode].lastSQLObject.sql;
        //             console.log('other query' + query);
        //             console.log('RESULT VALUE OF INSERT: ' + result + ' ENCLOSED');
        //             await dbs[otherNode].commit();
        //             console.log('after insert commit insert ther');
        //             await dbs[otherNode].insertOutbox(date, nodeNum, query);
        //             console.log('after insert outbox insert other');
        //             await dbs[otherNode].commit();
        //             console.log('after commit 2  insert ther');
        //             console.log('ERROR DEEZ NUTS THEN: ' + result + 'ENCLOSED');
        //             callback(true);
        //         })
        //         .catch(function (error) {
        //             console.log('ERROR DEEZ NUTS CATCH: ' + error + 'ENCLOSED');
        //             callback(false);
        //             //throw error; //both current node and the other node is down
        //         });
        // };

        // dbs[nodeNum]
        //     .insert(...params)
        //     .then(async function (result) {
        //         console.log('RESULT VALUE OF INSERT: ' + result);
        //         let query = dbs[nodeNum].lastSQLObject.sql;
        //         console.log('nodenum' + query);
        //         console.log('SQL MESSAGE TO BE SENT IS: ' + util.inspect(dbs[nodeNum].lastSQLObject.sql));
        //         await dbs[nodeNum].commit();
        //         await dbs[nodeNum].insertOutbox(date, otherNode, query);
        //         await dbs[nodeNum].commit();
        //         console.log('ERROR DEEZ NUTS: ' + JSON.stringify(result) + 'IM DEAD INSIDE');
        //         callback(result);
        //     })
        //     .catch(function (error) {
        //         let query = dbs[nodeNum].lastSQLObject.sql;
        //         console.log('catch query ' + query);
        //         console.log('there is an error: ' + error.message);
        //         insertOthers(nodeNum);
        //     });

        // if (year >= 1980) {
        //     //If valid node
        //     if (nodeNum != 2) {
        //         //Insert to the current node db
        //         if (nodeNum == 1) {
        //             let otherNode = 3;
        //             let date = new Date().toISOString().slice(0, 19).replace('T', ' ');
        //             // TODO: Change structure to switch-case
        //             //Attempt to insert on the current node database
        //             dbs[nodeNum]
        //                 .insert(id, name, year, rating, genre, director, actor_1, actor_2)
        //                 .then(async function (result) {
        //                     console.log('RESULT VALUE OF INSERT: ' + result);
        //                     //executed.sql
        //                     //If success, insert it into the outbox message
        //                     let message = dbs[nodeNum].connection;
        //                     await dbs[nodeNum].commit();
        //                     await dbs[nodeNum].insertOutbox(date, otherNode);
        //                     await dbs[nodeNum].commit();
        //                 })
        //                 .catch(async function () {
        //                     await dbs[nodeNum].rollback();
        //                 });
        //         } else {
        //         }
        //     }
        //If invalid node
        // else {
        //     //Send messages to two other nodes
        // }
        // } else {
        //     //If valid node
        //     if (nodeNum != 2) {
        //         if (nodeNum == 1) {
        //         } else {
        //         }
        //     }
        //     //If invalid node
        //     else {
        //         //Send messages to two other nodes
        //     }
        // }
    },

    find: function (id, callback) {
        dbs[nodeNum].find(id).then(function (result) {
            console.log('RESULT VALUE OF find: ');
            console.log(result);
            callback(result);
        });
    },

    findAll: function (callback) {
        console.log('hi');
        console.log(nodeNum + typeof nodeNum + '1');
        switch (parseInt(nodeNum)) {
            case 1:
                console.log(nodeNum + typeof nodeNum + '1');
                //query from database 1
                dbs[1]
                    .findAll()
                    .then(async function (result) {
                        console.log('before commit');
                        console.log(result);
                        commitResult = await dbs[1].commit();
                        //wait for commit result
                        console.log('commit: ' + JSON.stringify(commitResult));
                        callback(result);
                    })
                    .catch(async function (error) {
                        dbs[1].rollback();
                        if (error == Dao.MESSAGES.UNCONNECTED) {
                            let results = await Promise.all([dbs[2].findAll(), dbs[3].findAll()]);
                            await dbs[1].commit();
                            callback(result[0].concat(result[1]));
                        }
                    })
                    .catch(function (error) {
                        //all servers are down :/
                    });
            case 2:

            case 3:
        }
    },

    searchMovie: async function (name, callback) {
        let result = await dbs[1]
            .searchMovie(name)
            .catch(function (result) {
                return dbs[2].searchMovie(name);
            })
            .catch(function (result) {
                return dbs[3].searchMovie(name);
            })
            .catch(function (result) {
                // all servers down :/
            });
        callback(result);
    },

    update: function (id, name, year, rating, genre, director, actor_1, actor_2, callback) {
        if (year >= 1980) {
        }
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
