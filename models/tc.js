const axios = require('axios');

var siteUrls = [];

/** Sends a query to given nodes
 * @param   {String}      query   Query string to send
 * @param   {Number}      year    Year of associated with query
 * @param   {Number[]}    sites   Sites to message, default all sites
*/
function sendQuery (query, year, sites = Array(siteUrl.length).fill().map((n, i) => i)) {
    const options = {
        timeout: 20000
    };
    const data = {
        query: query,
        year: year
    }

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
     * @param   {}  query   Query to be executed, must be an XA transaction
     * @param   {}  xid     Unique name of the transaction
     * @param   {}  year    Year that the transaction effects
     */
    tryCommit: function (query, xid, year) {
        let livingSites = [];
        
        sendQuery(query, year)
        // The first phase of the two-phase commit - Sending the prepare messages to the appropriate nodes
        // res returns PromiseSettledResult
        .then(function(res) {
            // only message living sites
            for (let i = 0; i <= res.length; i++)
                if (res[i].status == "fulfilled" && res[i].value.data != "INCOMPATIBLE")
                    livingSites.push(i);

            // create log prepare
            return sendQuery(`XA PREPARE ${xid}`, year, livingSites);
        })
        // The second phase of the two-phase commit - Sending the commit messages to the appropriate nodes
        .then(function(res) {
            // Check responses if abort
            for (let i = 0; i <= res.length; i++) {
                if (res[i].status == "rejected" || res[i].value.data == "ABORT" /*&& notFailed*/) {
                    abort = true;
                    livingSites.splice(i, 1);
                }
            }
            if (abort) {
                // write abort T to logs
                return sendQuery(`XA ABORT ${xid}`, year, livingSites);
            }
            else {
                // write commit T to logs
                return sendQuery(`XA COMMIT ${xid}`, year, livingSites);
            }
        })
    },
}

module.exports = tc;