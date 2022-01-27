const controller = {
    getIndex: function (req, res) {
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
