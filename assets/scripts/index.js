$(document).ready(function () {
    // $('#results').DataTable({
    //     searching: false,
    //     oLanguage: {
    //         sSearch: '<i class="fa fa-search" style="color:#0074d9"></i>',
    //         // oAria: {
    //         //     sSortAscending: <i class="fa fa-sort-desc"></i>,
    //         // },
    //         oPaginate: {
    //             sNext: '<i class="fa fa-forward"></i>',
    //             sPrevious: '<i class="fa fa-backward"></i>',
    //             sFirst: '<i class="fa fa-step-backward"></i>',
    //             sLast: '<i class="fa fa-step-forward"></i>',
    //         },
    //     },
    //     pagingType: 'full_numbers',
    // });
    console.log(window.location);

    //TODO: Get the result then determine next button
    //If no result exists
    checkNoResults();
    //TODO: Get the page then determine prev button
    checkStartPage();

    insertSearchQuery();
});

function checkNoResults() {
    if ($('#no-results').length) $('#page-next').css('visibility', 'hidden');
}

function checkStartPage() {
    if ($('#page-cur').text() == '1') $('#page-prev').css('visibility', 'hidden');
}

function insertSearchQuery() {
    console.log();
    $('.page-link').each(function () {
        console.log('HREF ' + $(this).attr('href'));
        let href = $(this).attr('href') + window.location.search;
        $(this).attr('href', href);
        // $(this).setAttribute(href, $(this).href + window.location.search);
    });
}
