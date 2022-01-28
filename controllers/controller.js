const db = require('../models/db.js');

const controller = {
    getIndex: function (req, res) {
        query = 'SELECT * FROM test';
        db.query(query, function (result) {
            console.log(result);
        });
        res.render('index');
    },

    getViewPage: function (req, res) {
        res.render('view');
    },

    getCreatePage: function (req, res) {
        res.render('create');
    },

    getEditPage: function (req, res) {
        res.render('edit');
    },
};

module.exports = controller;
