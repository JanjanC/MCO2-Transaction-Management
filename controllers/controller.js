const db = require('../models/db.js');

const controller = {
    getIndex: function (req, res) {
        db.findAll(function (result) {
            if (result) {
                res.render('index', { movies: result });
            } else {
                res.redirect('/error');
            }
        });
    },

    getViewPage: function (req, res) {
        db.find(req.params.movie_id, function (result) {
            if (result) {
                res.render('view', result[0]);
            } else {
                res.redirect('/error');
            }
        });
    },

    getCreatePage: function (req, res) {
        res.render('create');
    },

    getEditPage: function (req, res) {
        db.find(req.params.movie_id, function (result) {
            if (result) {
                if (result[0].genre) {
                    let genre = result[0].genre.replace('-', '').replace(' ', '').toLowerCase();
                    result[0][genre] = result[0].genre;
                }
                if (result) {
                    res.render('edit', result[0]);
                }
            } else {
                res.redirect('/error');
            }
        });
    },

    getDelete: function (req, res) {
        db.delete(req.params.movie_id, function (result) {
            if (result) {
                res.redirect('/');
            } else {
                res.redirect('/error');
            }
        });
    },

    getSearchMovies: function (req, res) {
        db.searchMovie(req.query.movie_name, function (result) {
            if (result) {
                res.render('index', { movies: result });
            } else {
                res.redirect('/error');
            }
        });
    },

    getError: function (req, res) {
        res.render('error');
    },

    postCreate: function (req, res) {
        db.insert(
            req.body['movies-name'],
            req.body['movies-year'],
            req.body['movies-rating'],
            req.body['movies-genre'],
            req.body['director-name'],
            req.body['actor1-name'],
            req.body['actor2-name'],
            function (result) {
                console.log(result);
                if (result) {
                    res.redirect(`/view/${result}`);
                } else {
                    res.redirect('/error');
                }
            }
        );
    },

    postEdit: function (req, res) {
        // const values = {};
        // values[db.columns.name] = req.body['movies-name'];
        // values[db.columns.year] = req.body['movies-year'];
        // values[db.columns.rating] = req.body['movies-rating'];
        // values[db.columns.genre] = req.body['movies-genre'];
        // values[db.columns.director] = req.body['director-name'];
        // values[db.columns.actor_1] = req.body['actor1-name'];
        // values[db.columns.actor_2] = req.body['actor2-name'];

        db.update(
            req.params.movie_id,
            req.body['movies-name'],
            req.body['movies-year'],
            req.body['movies-rating'],
            req.body['movies-genre'],
            req.body['director-name'],
            req.body['actor1-name'],
            req.body['actor2-name'],
            function (result) {
                if (result) {
                    res.redirect('/view/' + req.params.movie_id);
                } else {
                    res.redirect('/error');
                } //send error or smth
            }
        );
    },
};

module.exports = controller;
