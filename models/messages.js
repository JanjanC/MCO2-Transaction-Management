const messages = {
    dbOps: {
        XATRANSACTION: 100,
        XAPREPARE: 101, 
        XACOMMIT: 102,
        XAROLLBACK: 103,
    },
    promiseStat: {
        REJECTED: 'rejected',
        FULFILLED: 'fulfilled',
    },
    serverStat: {
        INCOMPATIBLE: 200,
        ABORT: 201,
        READY: 202,
        TIMED_OUT: 203,
        DOWN: 203
    },
    errorMsg: {
        NO_SITES: "No sites to message"
    }
}

module.exports = messages