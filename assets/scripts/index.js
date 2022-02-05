$(document).ready(function () {
    $('#results').DataTable({
        searching: false,
        oLanguage: {
            sSearch: '<i class="fa fa-search" style="color:#0074d9"></i>',
            // oAria: {
            //     sSortAscending: <i class="fa fa-sort-desc"></i>,
            // },
            oPaginate: {
                sNext: '<i class="fa fa-forward"></i>',
                sPrevious: '<i class="fa fa-backward"></i>',
                sFirst: '<i class="fa fa-step-backward"></i>',
                sLast: '<i class="fa fa-step-forward"></i>',
            },
        },
        pagingType: 'full_numbers',
    });
});
