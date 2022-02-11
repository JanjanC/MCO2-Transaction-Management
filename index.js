const dotenv = require('dotenv');
const express = require('express');
const bodyParser = require('body-parser');
const hbs = require('hbs');
const routes = require('./routes/routes.js');
const path = require('path');
const db = require(`./models/db.js`);

const app = express();

app.use(bodyParser.urlencoded({ extended: false }));
app.use(express.static(path.join(__dirname, 'assets')));

app.set('view engine', 'hbs');
app.set('views', path.join(__dirname, 'views'));
app.use('/', routes);
hbs.registerPartials(path.join(__dirname, 'views/partials'));

dotenv.config();

db.connect(process.env.NODE_NUM, function () {
    app.use(express.static(path.join(__dirname)));

    app.get('/', function (req, res) {
        res.sendFile(path.join(__dirname, '/index.html'));
    });

    app.get('/test', function (req, res) {
        res.send(tc.tryCommit("HELLO WORLD", "abc123", 1890));
    });

    app.post('/send-query', function (req, res) {
        console.log(req.body);
        let timeoutId;
        let timedOut;
        if (req.body.query == dbOps.XATRANSACTION) {
            timedOut = false;
            timeoutId = setTimeout(function() {
                // if the requester doesnt send another message else in 50 second consider
                // the first function timed out
                timedOut = true;
            }, 50000)
        }
        else if (timeoutId != undefined) {
            clearTimeout(timeoutId);
            timeoutId = setTimeout(function() {
                // if the server doesnt send something else in 20 second consider
                // the first function timed out
                timedOut = true;
            }, 50000)
        }

        if (timedOut === true) {
            // timed out means the transaction manager died
            // roll back changes to the db
            res.send(serverStat.TIMED_OUT);
        }

        if (req.body.year != null &&
            (process.env.NODE_NUM == 2 && req.body.year < 1980) || 
            (process.env.NODE_NUM == 3 && req.body.year >= 1980))
            res.send(serverStat.INCOMPATIBLE);
        else if (!db.isDown)
            res.send(serverStat.DOWN);
        else {
            // execute query
            const data = req.body.data;
            let operation;
            switch (req.body.query) {
                case dbOps.XATRANSACTION:
                    operation = db.xaTransaction(data.query, data.xid, data.queryData);
                    break;
                case dbOps.XAPREPARE:
                    operation = db.xaPrepare(data.xid);
                    break;
                case dbOps.XACOMMIT:
                    operation = db.xaCommit(data.xid);
                    break;
                case dbOps.XAROLLBACK:
                    operation = db.xaRollback(data.xid);
                    break;
            };
            operation.then(function (result) {
                // log ready T
                // force all records for t into stable storage
                console.log(result);
                res.send(serverStat.READY);
            }) 
            .catch(function (error) {
                // log no T
                console.log(error);
                res.send(serverStat.ABORT);
            });
        }
    });

    app.listen(process.env.PORT || 3000, function () {
        console.log('Server running at Port ' + process.env.PORT);
    });
});