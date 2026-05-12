
/**
 * Creates a badge element for a given job state.
 *
 * @param {string} state Job state.
 * @returns {HTMLSpanElement} Badge element.
 */
function getStatusElement(state) {

    let badge = document.createElement('span');

    if (state === 'started') {
        badge.className = 'badge bg-warning';
        badge.innerHTML = 'started';
    } else if (state === 'success') {
        badge.className = 'badge bg-success';
        badge.innerHTML = 'success';
    } else if (state === 'error') {
        badge.className = 'badge';
        badge.innerHTML = 'error';
        badge.style.backgroundColor = 'purple';
    } else if (state === 'failed') {
        badge.className = 'badge bg-danger';
        badge.innerHTML = 'failed';
    } else {
        badge.className = 'badge bg-secondary';
        badge.innerHTML = 'unknown';
    }
    return badge;
}


