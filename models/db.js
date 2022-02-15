const dotenv = require('dotenv');
const { Dao } = require('../models/dao');
const mysql = require('mysql');

dotenv.config();

const INTERVAL = 90000;

var nodeNum;
let pools = [];

async function monitorOutbox() {
    let dbs = await getConnections();
    const query = `SELECT * FROM ${Dao.tables.outbox} WHERE ${Dao.outbox.status} = ?`;
    let messages;
    try {
        await dbs[nodeNum].startTransaction();
        messages = await dbs[nodeNum].query(query, [Dao.MESSAGES.UNACKNOWLEDGED]); //Fetch all the UNACKNOWLEDGED rows
        await dbs[nodeNum].commit();
    } catch {
        setTimeout(monitorOutbox, INTERVAL);
        return;
    }
    //console.log('data');
    for (let i of messages) {
        //console.log(i);
        try {
            await dbs[nodeNum].startTransaction();
            await dbs[i[Dao.outbox.recipient]].startTransaction();
        } catch (error) {
            console.log('error in start transaction outbox ' + error);
            break;
        }

        await dbs[i[Dao.outbox.recipient]]
            .insertInbox(i[Dao.outbox.id], nodeNum, i[Dao.outbox.message])
            .then(async () => {
                await dbs[nodeNum].setMessageStatus(i[Dao.inbox.id], Dao.tables.outbox, Dao.MESSAGES.SENT);
                await Promise.all([dbs[i[Dao.outbox.recipient]].commit(), dbs[nodeNum].commit()]);
            })
            .catch(async (error) => {
                console.error(error);
                if (!dbs[nodeNum].isDown) await dbs[nodeNum].rollback();
                if (!dbs[i[Dao.outbox.recipient]].isDown) await dbs[i[Dao.outbox.recipient]].rollback();

                //ERRNO 1062 = DUPLICATE of a row trying to insert; The message is already existing in the inbox of the otherNode
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
                console.log('error in outbox: ' + error);
            });
    }
    releaseConnections(dbs);
    setTimeout(monitorOutbox, INTERVAL);
}

async function monitorInbox() {
    let dbs = await getConnections();
    const query = `SELECT * 
                    FROM ${Dao.tables.inbox} 
                    WHERE ${Dao.inbox.status} =  ?
                    ORDER BY ${Dao.inbox.id} ASC`;
    let messages;
    try {
        await dbs[nodeNum].startTransaction();
        messages = await dbs[nodeNum].query(query, [Dao.MESSAGES.UNACKNOWLEDGED]); //Retrieves the list of unacknowledged messages in the inbox
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
            .startTransaction()
            //If the transaction is successful...
            .then(async function () {
                await dbs[nodeNum].query(i[Dao.inbox.message]); //Executes the 'query' inside the inbox
                await dbs[nodeNum].setMessageStatus(i[Dao.inbox.id], Dao.tables.inbox, Dao.MESSAGES.ACKNOWLEDGED); //Set acknowledged to your own inbox
                await dbs[i[Dao.inbox.sender]].startTransaction(); //Starts the transaction for the other DB
                await dbs[i[Dao.inbox.sender]].setMessageStatus(i[Dao.outbox.id], Dao.tables.outbox, Dao.MESSAGES.ACKNOWLEDGED); //Set acknowledge to the outbox of the sender
                await Promise.all([dbs[nodeNum].commit(), dbs[i[Dao.inbox.sender]].commit()]); //Commit once it is queried and acknowledge ---> ENDS THE TRANSACTION
                maintainDbConstraints(dbs)
            })
            //Else if it failed
            .catch(async function (error) {
                if (!dbs[nodeNum].isDown) await dbs[nodeNum].rollback();
                if (!dbs[i[Dao.inbox.sender]].isDown) await dbs[i[Dao.inbox.sender]].rollback();
                //ERRNO 1062 = DUPLICATE of a row trying to insert; If a message contains an INSERT to a row that's already been inserted to IMDB.js
                if (error.errno == 1062) {
                    // duplicate key entry
                    try {
                        await dbs[nodeNum].startTransaction();
                        await dbs[nodeNum].setMessageStatus(i[Dao.inbox.id], Dao.tables.inbox, Dao.MESSAGES.ACKNOWLEDGED);
                        await dbs[nodeNum].commit();
                    } catch {
                        if (!dbs[nodeNum].isDown) await dbs[nodeNum].rollback();
                    }
                }
                console.log('catch monitor inbox ' + error);
                //If the current nodeNum is down, it can be implied that the transaction is unsuccessful and an automatic rollback was executed
            });
    }
    releaseConnections(dbs);
    setTimeout(monitorInbox, INTERVAL);
}

/**
 * Creates new pool connections so as to avoid the issue of having "shared" connections
 */
async function getConnections() {
    let dbs = {};
    let inits = [];
    for (let i = 1; i <= 3; i++) {
        dbs[i] = new Dao(i - 1);
        inits.push(dbs[i].initialize(pools[i - 1]));
    }
    await Promise.allSettled(inits);
    return dbs;
}

/**
 * Releases pool connections so as to recycle the spots in the overall pool, avoiding the issue of reaching max capacity of pools and/or creating new pools which takes a lot of time
 */
function releaseConnections(dbs) {
    for (let i = 1; i <= 3; i++) {
        if (dbs[i].connection && !dbs[i].isDown) {
            try {
                dbs[i].connection.release();
            } catch (error) {
                console.log('realase connection error: ' + error.stack);
            }
        }
    }
}

async function sendMessages (results, dbs, query) {
    let failedSites = [];
    let workingSites = [];
    for (let i of results) {
        if (i.status == 'rejected' /*&& dbs[i.reason].isDown*/) failedSites.push(i.reason);
        //else if (i.status == 'rejected') workingSites.push(i.reason);
        else workingSites.push(i.value);
    }

    console.log('failed sites: ' + failedSites);
    console.log('working sites: ' + workingSites);

    let inboxQuery = `INSERT INTO ${Dao.tables.outbox} (${Dao.outbox.id}, ${Dao.outbox.recipient}, ${Dao.outbox.message}, ${Dao.outbox.status})
                        VALUES(?, ?, ?, ?)`;

    // For each failed site, try to write to outbox, if unable rollback everything and send error
    let sentAny = failedSites.length == 0 ? true : false;
    for (let i of failedSites) {
        let messagesToSend = workingSites.map(function (value) {
            return dbs[value].query(inboxQuery, [new Date().getTime(), i, query, Dao.MESSAGES.UNACKNOWLEDGED]);
        });
        
        let messageResults = await Promise.allSettled(messagesToSend)
        messageResults.forEach(function (result) {
            if (result.status == "rejected")
                console.log(result.reason);
            else
                sentAny = true
        });
        if (!sentAny)
            break;
    }

    //sentAny = false;

    if (sentAny) {
        try {
            let commitAll = workingSites.map(function(value) {
                return dbs[value].commit()
            })
            console.log("committed")
            await Promise.all(commitAll);
            return true;
        } catch (error) {
            console.log(error);
            console.log("rolled back failed to commit")
            let rollbackAll = workingSites.map(function(value) {
                return dbs[value].rollback()
            })
            await Promise.allSettled(rollbackAll);           
            return false;
        }
    }
    else {
        //let sleep = (ms) => {return new Promise(resolve => setTimeout(resolve, ms));}
        //await sleep(7500);
        let rollbackAll = workingSites.map(function(value) {
            return dbs[value].rollback()
        })
        //console.log("FORCING A ROLLBACK");
        //console.log("rolled back failed to send messages")
        await Promise.allSettled(rollbackAll);
        return false;
    }
}

async function maintainDbConstraints(dbs) {
    const removeGreaterThan = `
        DELETE FROM ${Dao.tables.imdb}
        WHERE ${Dao.imdb.year} >= 1980
    `;
    const removeLessThan = `
        DELETE FROM ${Dao.tables.imdb}
        WHERE ${Dao.imdb.year} < 1980
    `;
    await Promise.allSettled([dbs[2].query(removeGreaterThan), dbs[3].query(removeLessThan)]);
}

const db = {
    /*
    node - the current node number of the server
    */
    connect: async function (node) {
        nodeNum = node;
        for (let i = 0; i < 3; i++) pools.push(mysql.createPool(Dao.NODES[i]));
        monitorOutbox();
        await monitorInbox();
    },

    // TODO: Configure the node and target the special scenario of either sending two messages, or sending to node 1 first
    insert: async function (name, year, rating, genre, director, actor_1, actor_2, callback) {
        let dbs = await getConnections();
        let params = [new Date().getTime(), name, year, rating, genre, director, actor_1, actor_2];
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
        if (await sendMessages(results, dbs, query)) {
            releaseConnections(dbs);
            callback(index);
        }
        else {
            releaseConnections(dbs);
            callback(0);
        }
    },

    find: async function (id, callback) {
        let dbs = await getConnections();
        let queryDb1 = () => {
            return dbs[1].find(id).then(async function (result) {
                await dbs[1].commit();
                //console.log(await dbs[1].query("SELECT @@transaction_isolation;"));
                console.log('call back in db1 query');

                callback(result);
            });
        };
        //Node 2/3 must check in both Node 2 and 3, because the year (and node #) cannot be determined through the id alone
        let queryDb2And3 = async () => {
            const invert  = p  => new Promise((res, rej) => p.then(rej, res));
            const firstOf = ps => invert(Promise.all(ps.map(invert)));

            let nodesToFind = [2, 3].map(function (value) {
                return dbs[value].find(id).then(async function (result) {
                    console.log('db ' + value + ' got result ' + JSON.stringify(result));
                    //console.log(await dbs[1].query("SELECT @@transaction_isolation;"));
                    if (typeof result !== 'undefined' && result.length == 0) return Promise.reject('Did not find');
                    await dbs[value].commit();
                    return Promise.resolve(result);
                });
            });
            return firstOf(nodesToFind).then(function (result) {
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
                    await Promise.allSettled([dbs[2].rollback(), dbs[3].rollback()]);
                    callback(false);
                });
            });
        } else {
            queryDb2And3().catch(async function (error) {
                console.log(error);
                await Promise.allSettled([dbs[2].rollback(), dbs[3].rollback()]);
                await queryDb1().catch(async function () {
                    if (!dbs[1].isDown) await dbs[1].rollback();
                    callback(false);
                });
            });
        }

        releaseConnections(dbs);
    },

    //TODO: ID NODE 2 SEARCH NODE 2 / 3 FIRST
    searchMovie: async function (name, page, callback) {
        let dbs = await getConnections();
        let queryDb1 = () => {
            return dbs[1].searchMovie(name, page).then(async function (result) {
                await dbs[1].commit();
                callback(result);
                return Promise.resolve('its fine');
            });
        };
        let queryDb2And3 = async () => {
            return Promise.all([dbs[2].searchMovie(name, page), dbs[3].searchMovie(name, page)]).then(async function (result) {
                await Promise.all([dbs[2].commit(), dbs[3].commit()]);
                callback(result[0].concat(result[1]));
                return Promise.resolve('its fine');
            });
        };

        if (nodeNum == 1) {
            queryDb1()
                .catch(async function () {
                    if (!dbs[1].isDown) await dbs[1].rollback();
                    return queryDb2And3();
                })
                .catch(async function () {
                    await Promise.allSettled([dbs[2].rollback(), dbs[3].rollback()]);
                    callback(false);
                });
        } else {
            queryDb2And3()
                .catch(async function () {
                    await Promise.allSettled([dbs[2].rollback(), dbs[3].rollback()]);
                    return queryDb1();
                })
                .catch(async function () {
                    if (!dbs[1].isDown) await dbs[1].rollback();
                    callback(false);
                });
        }

        releaseConnections(dbs);
    },

    findAll: async function (page, callback) {
        let dbs = await getConnections();
        let queryDb1 = () => {
            return dbs[1].findAll(page).then(async function (result) {
                await dbs[1].commit();
                callback(result);
                return Promise.resolve('its fine');
            });
        };
        let queryDb2And3 = async () => {
            return Promise.all([dbs[2].findAll(page), dbs[3].findAll(page)]).then(async function (result) {
                await Promise.all([dbs[2].commit(), dbs[3].commit()]); //Wait for both of the find to successfully commit
                callback(result[0].concat(result[1]));
                return Promise.resolve('its fine');
            });
        };

        if (nodeNum == 1) {
            queryDb1()
                .catch(async function () {
                    if (!dbs[1].isDown) await dbs[1].rollback();
                    return queryDb2And3();
                })
                .catch(async function () {
                    await Promise.allSettled([dbs[2].rollback(), dbs[3].rollback()]);
                    callback(false);
                });
        } else {
            queryDb2And3()
                .catch(async function () {
                    await Promise.allSettled([dbs[2].rollback(), dbs[3].rollback()]);
                    return queryDb1();
                })
                .catch(async function () {
                    if (!dbs[1].isDown) await dbs[1].rollback();
                    callback(false);
                });
        }

        releaseConnections(dbs);
    },

    update: async function (id, name, year, rating, genre, director, actor_1, actor_2, changed, callback) {
        let dbs = await getConnections();
        let params = [id, name, year, rating, genre, director, actor_1, actor_2, changed];
        let query = '';
        let index = params[0];

        // Creates an array of promises for each node that contains a replica
        let nodesToInsert = [1, 2, 3].map(function (value) {
            //So that it returns it without executing it
            return dbs[value]
                .update(...params)
                .then(function (result) {
                    query = dbs[value].lastSQLObject.sql;
                    return Promise.resolve(value);
                })
                .catch(function (error) {
                    return Promise.reject(value);
                });
        });

        let results = await Promise.allSettled(nodesToInsert);
        await maintainDbConstraints(dbs);
        if (await sendMessages(results, dbs, query)) {
            releaseConnections(dbs);
            callback(index);
        }
        else {
            releaseConnections(dbs);
            callback(0);
        }
    },

    delete: async function (id, callback) {
        let dbs = await getConnections();
        let query = '';

        // Creates an array of promises for each node that contains a replica
        let nodesToInsert = [1, 2, 3].map(function (value) {
            return dbs[value]
                .delete(id)
                .then(function (result) {
                    query = dbs[value].lastSQLObject.sql;
                    return Promise.resolve(value);
                })
                .catch(function (error) {
                    return Promise.reject(value);
                });
        });

        let results = await Promise.allSettled(nodesToInsert);

        if (await sendMessages(results, dbs, query)) {
            releaseConnections(dbs);
            callback(index);
        }
        else {
            releaseConnections(dbs);
            callback(0);
        }
    },

    generateReport: async function (callback) {
        let allQueries = {
            tot_movies: `SELECT COUNT(*) AS count FROM ${Dao.tables.imdb}`, //Total Number of Movies
            avg_rating: `SELECT AVG(${Dao.imdb.rating}) AS avg FROM ${Dao.tables.imdb}`, //Average Rating of all Movies
            tot_per_genre: `SELECT ${Dao.imdb.genre}, COUNT(*) AS count FROM ${Dao.tables.imdb} GROUP BY ${Dao.imdb.genre} ORDER BY ${Dao.imdb.genre}`, //Total Number of Movies Per Genre
            avg_per_genre: `SELECT ${Dao.imdb.genre}, AVG(${Dao.imdb.rating}) AS avg FROM ${Dao.tables.imdb} GROUP BY ${Dao.imdb.genre} ORDER BY ${Dao.imdb.genre}`, //Avg. Rating Per Genre
            tot_per_year: `SELECT ${Dao.imdb.year}, COUNT(*) AS count FROM ${Dao.tables.imdb} GROUP BY ${Dao.imdb.year} ORDER BY ${Dao.imdb.year}`, //Total Number Per Year
            avg_per_year: `SELECT ${Dao.imdb.year}, AVG(${Dao.imdb.rating}) AS avg FROM ${Dao.tables.imdb} GROUP BY ${Dao.imdb.year} ORDER BY ${Dao.imdb.year}`, //Avg. Rating Per Year
        };

        const combinedItems = (array, query) => {
            if (query == allQueries.tot_per_year || query == allQueries.avg_per_year) {
                return array;
            }
            else if (query == allQueries.tot_movies || query == allQueries.avg_rating) {
                return array.reduce(function (prev, i) {
                    if (i.count)
                        return prev + i.count;
                    else
                        return prev + i.avg;
                }, 0);
            }
            else {
                let repeatedGenres = {}
                let newArray = []
                for (let i of array) {
                    if (Object.keys(repeatedGenres).indexOf(i.genre)) {
                        if (i.count)
                            newArray[repeatedGenres[i.genre]] += i.count;
                        else
                            newArray[repeatedGenres[i.genre]] += i.avg;
                    }
                    else {
                        if (i.count)
                            newArray.push({}[i.genre] = i.count);
                        else
                            newArray.push({}[i.genre] = i.avg);
                        repeatedGenres[i.genre] = newArray.length - 1;
                    }
                }
                return newArray; // return combined genre counts
            }
        };

        let dbs = [];
        for (let i in allQueries) dbs.push(await getConnections());
        let queryDb1 = async (queries) => {
            let execs = queries.map(function (value, index) {
                return dbs[index][1].startTransaction().then(async function () {
                    let result = await dbs[index][1].query(value);
                    await dbs[index][1].commit();
                    return Promise.resolve(result);
                });
            });
            return Promise.allSettled(execs);
        };
        let queryDb2And3 = async (queries) => {
            let execs = queries.map(function (value, index) {
                return Promise.all([dbs[index][2].startTransaction(), dbs[index][3].startTransaction()]).then(async function () {
                    let result = await Promise.all([dbs[index][2].query(value), dbs[index][3].query(value)]);
                    await Promise.all([dbs[index][2].commit(), dbs[index][3].commit()]);
                    console.log('----->' + JSON.stringify(result[0]));
                    console.log('----->' + JSON.stringify(result[1]));
                    return Promise.resolve(combinedItems(result[0].concat(result[1]), query));
                });
            });
            return Promise.allSettled(execs);
        };
        let keys = Object.keys(allQueries);
        let values = Object.values(allQueries);
        let finalResults = {};
        let failedQueries = {};
        if (nodeNum == 1) {
            queryDb1(values)
                .then(async function (result) {
                    for (let i = 0; i < result.length; i++) {
                        if (result[i].status == 'rejected') {
                            console.log('1st then of db1: ' + result[i].reason);
                            if (!dbs[i][1].isDown) await dbs[i][1].rollback();
                            failedQueries[keys[i]] = values[i];
                        } else finalResults[keys[i]] = result[i].value;
                    }
                    return queryDb2And3(Object.values(failedQueries));
                })
                .then(async function (result) {
                    for (let i = 0; i < result.length; i++) {
                        if (result[i].status == 'rejected') {
                            console.log('2nd then of 1: ' + result[i].reason);
                            await Promise.allSettled([dbs[i][2].rollback(), dbs[i][3].rollback()]);
                        } else finalResults[Object.keys(failedQueries)] = result[i].value;
                    }
                    if (Object.entries(finalResults).length === 0) throw new Error('All dbs down');
                    else {
                        callback(finalResults);
                        dbs.forEach(releaseConnections);
                    }
                })
                .catch(function (error) {
                    console.log('report db1 catch ' + error.stack);
                    callback(false);
                    dbs.forEach(releaseConnections);
                });
        } else {
            queryDb2And3(values)
                .then(async function (result) {
                    let failedQueries = [];
                    for (let i = 0; i < result.length; i++) {
                        if (result[i].status == 'rejected') {
                            await Promise.allSettled([dbs[i][2].rollback(), dbs[i][3].rollback()]);
                            console.log('1st then of db2&3: ' + result[i].reason);
                            failedQueries[keys[i]] = values[i];
                        } else finalResults[keys[i]] = result[i].value;
                    }
                    return queryDb1(Object.values(failedQueries));
                })
                .then(async function (result) {
                    for (let i = 0; i < result.length; i++) {
                        if (result[i].status == 'rejected') {
                            console.log('2nd then of db2&3: ' + result[i].reason);
                            if (!dbs[i][1].isDown) await dbs[i][1].rollback();
                        } else finalResults[Object.keys(failedQueries)] = result[i].value;
                    }
                    if (Object.entries(finalResults).length === 0) throw new Error('All dbs down');
                    else {
                        callback(finalResults);
                        dbs.forEach(releaseConnections);
                    }
                })
                .catch(function (error) {
                    console.log('report db2&3 catch ' + error.stack);
                    callback(false);
                    dbs.forEach(releaseConnections);
                });
        }
    },
};

module.exports = db;
