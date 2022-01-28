$("#add-actor").click(function() {
    $("#actors-div:last-child").remove()
    $("#actors-div").append(`
        <div class='row mt-4 border-bottom'>
            <div class='col'>
                <!--NAME-->
                <div class='form-group'>
                    <label><h4>FIRST NAME</h4></label>
                    <input
                        type='text'
                        class='form-control w-75'
                        placeholder="Enter actor's first name"
                        name='actor-fname'
                    />
                </div>
                <!--RANK-->
                <div class='form-group'>
                    <label><h4>LAST NAME</h4></label>
                    <input
                        type='text'
                        class='form-control w-75'
                        placeholder="Enter actor's last name"
                        name='actor-lname'
                    />
                </div>
            </div>

            <div class='col'>
                <!--GENDER-->
                <label><h4>GENDER</h4></label>
                <div class='p-3 border rounded row mr-2'>
                    <!--GENDER COLUMN 1-->
                    <div class='col'>
                        <!--GENDER-ITEM-->
                        <div class='form-check'>
                            <input
                                class='form-check-input'
                                type='radio'
                                value='M'
                                name='gender-item'
                            />
                            <label
                                class='form-check-label'
                            >
                                MALE
                            </label>
                        </div>
                    </div>
                    <!--GENDER COLUMN 2-->
                    <div class='col'>
                        <!--GENDER-ITEM-->
                        <div class='form-check'>
                            <input
                                class='form-check-input'
                                type='radio'
                                value='F'
                                name='gender-item'
                            />
                            <label
                                class='form-check-label'
                            >
                                FEMALE
                            </label>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        <div class='row mt-4'>
            <button type='button' class='btn' id="add-actor">Add Another Actor</button>
        </div>
    `)
})

$("#add-director").click(function() {
    $("#directors-div:last-child").remove()
    $("#directors-div").append(`
        
        <div class='row mt-4'>
            <button type='button' class='btn' id="add-actor">Add Another Actor</button>
        </div>
    `)
})