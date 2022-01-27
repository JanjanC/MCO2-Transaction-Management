const express = require('express');
const controller = require('../controllers/controller.js');

const app = express();

app.get('/', controller.getIndex);
app.get('/view', controller.getViewPage);
app.get('/create', controller.getCreatePage);
app.get('/edit', controller.getEditPage);

module.exports = app;
