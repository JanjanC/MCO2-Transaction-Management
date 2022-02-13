const express = require('express');
const controller = require('../controllers/controller.js');

const app = express();

app.get('/', controller.getIndex);
app.get('/view/:movie_id', controller.getViewPage);
app.get('/create', controller.getCreatePage);
app.get('/edit/:movie_id', controller.getEditPage);
app.get('/delete/:movie_id', controller.getDelete);
app.get('/search', controller.getSearchMovies);
app.get('/report', controller.getReport);
app.get('/error', controller.getError);

app.post('/create', controller.postCreate);
app.post('/edit/:movie_id', controller.postEdit);

module.exports = app;
