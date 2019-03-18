//DOM -document object model
const toTravel = [{
    country: 'Switzerland',
    todo: 'ski'
}, {
    country: 'Sinai',
    todo: 'scuba Dive'
},
    {
        country: 'Italy',
        todo: 'eat pizza'
    }
]
document.querySelector('#create-note').addEventListener('click', function (e) {
    e.target.textContent = 'create-note button was clicked'
})
document.querySelector('#remove-all').addEventListener('click', function (e) {
    document.querySelectorAll('.note').forEach(function (note) {
        note.remove()
    })
    // document.querySelectorAll('#remove-all').
})
document.querySelector('#search-text').addEventListener('input',function (e) {
    console.log(e.target.value)

})