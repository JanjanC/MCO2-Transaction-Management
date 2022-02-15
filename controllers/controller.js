const db = require('../models/db.js');

const controller = {
    getIndex: function (req, res) {
        res.redirect('/page/1');
    },

    getIndexPage: function (req, res) {
        page = +req.params.page_num;
        db.findAll(page, function (result) {
            if (result) {
                res.render('index', { movies: result, page_prev: page - 1, page: page, page_next: page + 1 });
            } else {
                res.redirect('/error');
            }
        });
    },

    getViewPage: function (req, res) {
        db.find(req.params.movie_id, function (result) {
            console.log('get view page result' + JSON.stringify(result));
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
        page = +req.params.page_num;
        db.searchMovie(req.query.movie_name, page, function (result) {
            if (result) {
                res.render('index', { movies: result, page_prev: page - 1, page: page, page_next: page + 1 });
            } else {
                res.redirect('/error');
            }
        });
    },

    getReport: function (req, res) {
        db.generateReport(function (result) {
            console.log('-------->REPORT GENERATED<-----UWU---\n' + JSON.stringify(result));
            if (result) res.render('report', result);
            else res.redirect('/error');
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
        
        if (req.body['movies-genre'] == undefined)
            req.body['movies-genre'] = '';
        db.update(
            req.params.movie_id,
            req.body['movies-name'],
            parseInt(req.body['movies-year']),
            parseFloat(req.body['movies-rating']),
            req.body['movies-genre'],
            req.body['director-name'],
            req.body['actor1-name'],
            req.body['actor2-name'],
            {
                movie_name: req.body['movies-name'] !== req.body['old-name'],
                year: parseInt(req.body['movies-year']) !== parseInt(req.body['old-year']),
                rating: req.body['movies-rating'] !== req.body['old-rating'],
                genre: req.body['movies-genre'] !== req.body['old-genre'],
                director: req.body['director-name'] !== req.body['old-director'],
                actor_1: req.body['actor1-name'] !== req.body['old-actor-1'],
                actor_2: req.body['actor2-name'] !== req.body['old-actor-2'],
            },
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
