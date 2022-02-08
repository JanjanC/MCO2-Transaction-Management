const dotenv = require('dotenv');
const express = require('express');
const bodyParser = require('body-parser');
const hbs = require('hbs');
const routes = require('./routes/routes.js');
const path = require('path');
const db = require(`./models/db.js`);
const tc = require(`./models/tc.js`);

const app = express();

app.use(bodyParser.urlencoded({ extended: false }));
app.use(express.static(path.join(__dirname, 'assets')));

app.set('view engine', 'hbs');
app.set('views', path.join(__dirname, 'views'));
app.use('/', routes);
hbs.registerPartials(path.join(__dirname, 'views/partials'));

dotenv.config();

tc.initialize(process.env.NODE_NUM);

db.connect(process.env.NODE_NUM, function () {
    app.use(express.static(path.join(__dirname)));

    app.get('/', function (req, res) {
        res.sendFile(path.join(__dirname, '/index.html'));
    });

    app.get('/test', function (req, res) {
        res.send(tc.tryCommit("HELLO WORLD", "abc123", 1890));
    });

    app.get('/send-query', function (req, res) {
        console.log(req.body);
        //res.sendFile(path.join(__dirname, '/index.html'));
        if ((process.env.NODE_NUM == 2 && req.body.year >= 1980) || 
            (process.env.NODE_NUM == 3 && req.body.year < 1980))
            res.send("INCOMPATIBLE");
        // execute query
        // if not ready to commmit
        if (readyToCommitlmao() == true) {
            // log no T
            res.send("ABORT");
        }
        else {
            // log ready T
            // force all records for t into stable storage
            res.send("READY");
        }
        // else
    });

    app.listen(process.env.PORT || 3000, function () {
        console.log('Server running at Port ' + process.env.PORT);
    });
});