const dotenv = require('dotenv');
const express = require('express');
const bodyParser = require('body-parser');
const hbs = require('hbs');
const routes = require('./routes/routes.js');
const path = require('path');
const mysql = require('mysql');
const db = require(`./models/db.js`);

const { createServer } = require('http');
const { Server } = require('socket.io');

const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer, {});

app.use(bodyParser.urlencoded({ extended: false }));
app.use(express.static(path.join(__dirname, 'assets')));

app.set('view engine', 'hbs');
app.set('views', path.join(__dirname, 'views'));
app.use('/', routes);
hbs.registerPartials(path.join(__dirname, 'views/partials'));

dotenv.config();

db.connect(process.env.NODE, function () {
    app.use(express.static(path.join(__dirname)));

    app.get('/', function (req, res) {
        res.sendFile(path.join(__dirname, '/index.html'));
    });

    app.listen(process.env.PORT || 3000, process.env.HOSTNAME, function () {
        console.log('Server running at: ');
        console.log('http://' + process.env.HOSTNAME + `:` + process.env.PORT);
    });
});

// VVV socket stuff

// const io = require('socket.io')();
// io.listen(process.env.PORT);

// function handler(req, res) {
//     res.writeHead(200);
//     res.end();
// }

// var connectedClients = 0;
// var yesFromClients = 0;
// var ackFromClients = 0;

// io.on('connection', function () {
//     socket.on('coordinator', function (data) {
//         socket.join('coordinators');
//         console.log(socket.id + ': coordinator');
//     });

//     socket.on('prepare', function (data) {
//         io.sockets.in('clients').emit('prepare', data);
//         console.log(socket.id + ': prepare');
//     });

//     socket.on('commit', function (data) {
//         io.sockets.in('clients').emit('commit', data);
//         console.log(socket.id + ': commit');
//     });

//     socket.on('client', function (data) {
//         socket.join('clients');
//         connectedClients++;
//         io.sockets.in('coordinators').emit('newClient', { client: socket.id });
//         console.log(socket.id + ': client');

//         socket.on('yes', function (data) {
//             yesFromClients++;
//             if (yesFromClients == connectedClients) {
//                 io.sockets.in('clients').emit('commit', data);
//             }
//             io.sockets.in('coordinators').emit('yes', { client: socket.id });
//             console.log(socket.id + ': yes');
//         });

//         socket.on('ack', function (data) {
//             io.sockets.in('coordinators').emit('ack', { client: socket.id });
//             ackFromClients++;
//             if (ackFromClients == connectedClients) {
//                 ackFromClients = 0;
//                 yesFromClients = 0;
//             }
//             console.log(socket.id + ': ack');
//         });

//         socket.on('disconnect', function (data) {
//             io.sockets.in('coordinators').emit('clientDisconnect', { client: socket.id });
//             console.log(socket.id + ': disconnect');
//             connectedClients--;
//         });
//     });
// });
