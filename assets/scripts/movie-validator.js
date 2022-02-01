function validateInput() {
    let isValid = true;

    if ($('#movies-name').val() === '') {
        $('#name-error').text('Movie Name is a required field');
        isValid = false;
    } else {
        $('#name-error').text('');
    }

    // if ($('#movies-rating').val() === '') {
    //     $('#rating-error').text('Rating cannot be empty');
    //     isValid = false;
    // } else

    if ($('#movies-rating').val() && $('#movies-rating').val().toString().split('.')[1].length > 1) {
        $('#rating-error').text('Rating can only have 1 decimal place');
        isValid = false;
    } else $('#rating-error').text('');

    if ($('#movies-year').val() === '') {
        $('#year-error').text('Year is a required field');
        isValid = false;
    } else {
        $('#year-error').text('');
    }

    // if ($('#genres input[type=radio]:checked').length == 0) {
    //     $('#genre-error').text('A genre must be selected');
    //     isValid = false;
    // } else $('#genre-error').text('');

    // if ($('#actor1-name').val() === '') {
    //     $('#actor1-name-error').text('Actor 1 name cannot be empty');
    //     isValid = false;
    // } else $('#actor1-name-error').text('');

    // if ($('#actor2-name').val() === '') {
    //     $('#actor2-name-error').text('Actor 2 name cannot be empty');
    //     isValid = false;
    // } else $('#actor2-name-error').text('');

    // if ($('#director-name').val() === '') {
    //     $('#director-name-error').text('Director name cannot be empty');
    //     isValid = false;
    // } else $('#director-name-error').text('');

    return isValid;
}
