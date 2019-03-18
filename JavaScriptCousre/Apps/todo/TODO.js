const notes = [{
    text: 'clean room ',
    completed: true
}, {
    text: 'go to uni',
    completed: false
}, {
    text: 'eat lunch ',
    completed: false
}, {
    text: 'read contract ',
    completed: false
}, {
    text: 'take a shower ',
    completed: false
}]
const filters = {
    searchText: ''
}
const renderNotes = function (notes, filters) {
    const filterNotes = notes.filter(function (note) {
        return note.title.toLowerCase().includes(filters.searchText.toLowerCase())
    })
    console.log(filterNotes)
}

renderNotes(notes,filters)
const todoIncomplete = notes.filter(function (todo) {
    return !todo.completed
})
const summary = document.createElement('h2')
summary.textContent = 'Todo remaining :' + todoIncomplete.length + ' remaining'
document.querySelector('body').appendChild(summary)


document.querySelector('#new-todo').addEventListener('input', function (e) {
    filters.searchText=e.target.value
    renderNotes(notes,filters)

})