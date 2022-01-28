const dotenv = require('dotenv');
const express = require('express');
const bodyParser = require('body-parser');
const hbs = require('hbs');
const routes = require('./routes/routes.js');
const path = require('path');
const mysql = require('mysql');
const db = require(`./models/db.js`);

const app = express();

app.use(bodyParser.urlencoded({ extended: false }));
app.use(express.static(path.join(__dirname, 'assets')));

app.set('view engine', 'hbs');
app.set('views', path.join(__dirname, 'views'));
app.use('/', routes);
hbs.registerPartials(path.join(__dirname, 'views/partials'));

dotenv.config();

db.connect();

app.use(express.static(path.join(__dirname)));

app.get('/', function (req, res) {
    res.sendFile(path.join(__dirname, '/index.html'));
});

app.listen(process.env.PORT, process.env.HOSTNAME, function () {
    console.log('Server running at: ');
    console.log('http://' + process.env.HOSTNAME + `:` + process.env.PORT);
});
