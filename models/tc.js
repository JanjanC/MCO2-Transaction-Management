const axios = require('axios');
const {dbOps, promiseStat, serverStat, errorMsg: error} = require('../models/messages.js');
const util = require('util');

var siteUrls = [];

/** Sends a query to given nodes
 * @param   {String}      query   Query string to send
 * @param   {Number}      year    Year of associated with query
 * @param   {Number[]}    sites   Sites to message, default all sites
*/
function sendQuery (query, data, year, sites = Array(siteUrls.length).fill().map((n, i) => i)) {
    if (sites.length == 0)
        return [];
    const options = {
        timeout: 20000
    };
    const data = {
        query: query,
        data: data,
        year: year
    };    

    //Loads the messages to be executed in a promise
    const messages = sites.map(function (nodeNum) {
        return axios.post(siteUrls[nodeNum] + "/send-query", data, options);
    })

    //Firing Pin - Executes promises at the same time
    return Promise.allSettled(messages);
};

const tc = {
    /**Initializes which urls will be contacted
     * @param   {*}   node  The node number of this site
     */
    initialize: function(node) {
        // Order of url must match node numbering
        siteUrls = [
            'http://stadvdb-mco2-g4-node1.herokuapp.com',
            'http://stadvdb-mco2-g4-node2.herokuapp.com',
            'http://stadvdb-mco2-g4-node3.herokuapp.com'
        ]
        siteUrls.splice(node - 1, 1);
    },
    
    sendQuery: sendQuery,

    /** Executes the two-phase commit
     * @param   {string}    query   Query to be executed, must be an XA transaction
     * @param   {any}       data    Data to be passed for the query
     * @param   {string}    xid     Unique name of the transaction
     * @param   {number}    year    Year that the transaction effects
     */
    tryCommit: function (query, year, xid, queryData = {}) {
        let livingSites = [];
        return sendQuery(dbOps.XACOMMIT, {query: query, xid: xid, queryData: queryData}, year)
        // The first phase of the two-phase commit - Sending the prepare messages to the appropriate nodes
        // res returns PromiseSettledResult
        .then(function(res) {
            console.log("PREPARE RES: " + util.inspect(res))
            // only message living sites
            for (let i = 0; i < res.length; i++)
                if (res[i].status == promiseStat.FULFILLED && res[i].value.data != serverStat.INCOMPATIBLE)
                    livingSites.push(i);

            // create log prepare
            return sendQuery(dbOps.XAPREPARE, {xid: xid}, year, livingSites);
        })
        // The second phase of the two-phase commit - Sending the commit messages to the appropriate nodes
        .then(function(res) {
            console.log("COMMIT RES: " + util.inspect(res))
            // Check responses if abort
            let abort = false;
            for (let i = 0; i < res.length; i++) {
                if (res[i].status == promiseStat.REJECTED || res[i].value.data == serverStat.ABORT /*&& notFailed*/) {
                    abort = true;
                    livingSites.splice(i, 1);
                }
            }
            if (abort) {
                // write abort T to logs
                return sendQuery(dbOps.XAROLLBACK, {xid: xid}, year, livingSites);
            }
            else {
                // write commit T to logs
                return sendQuery(dbOps.XACOMMIT, {xid: xid}, year, livingSites);
            }
        })
    },
}

module.exports = tc;