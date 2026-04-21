/**
 * Restores SearchBuilder filters from the URL hash.
 *
 * @param {DataTable} anchor_table DataTable instance.
 * @returns {boolean|void} True when a filter was applied.
 */
function setFilterFromSharedURL(anchor_table) {
    // take the hash part of the shared URL
    // the hash part contains the filter criteria that were shared
    let hash = location.hash.replace('#', '');

    if (hash === '') {
        // this means a normal URL was opened
        // not a shared filter URL
        return;
    }

    let j = decodeURIComponent(hash);
    let d = JSON.parse(j);
    anchor_table.searchBuilder.rebuild(d);
    return true
};

/**
 * Converts SearchBuilder details to a shareable URL hash.
 *
 * @param {object} searchBuilderDetails SearchBuilder details object.
 * @returns {string} Encoded hash string or empty string.
 */
function getURLHashFromSearchBuilder(searchBuilderDetails) {
    let j = JSON.stringify(searchBuilderDetails);
    if (j === '{}') {
        return '';
    }
    return '#' + encodeURIComponent(j);
}

/**
 * Creates the DataTables button config for sharing current filters.
 *
 * @returns {object} DataTables button configuration.
 */
function filterSharingButton() {
    return {
        text: 'Share',
        action: (e, dt, node, config) => {
            // save the current filter state as a parameter in a URL
            let hashValue;
            try {
                hashValue = getURLHashFromSearchBuilder(dt.searchBuilder.getDetails());
            } catch (error) {
                globalThis.alert('Error: ' + error);
                return;
            }
            let full_url = globalThis.location.origin + globalThis.location.pathname + hashValue;

            if (navigator.clipboard && navigator.clipboard.writeText) {
                // copy URL to the clipboard and display a feedback message
                navigator.clipboard.writeText(full_url).then(() => {
                    dt.buttons.info(
                        'Filter URL copied to clipboard',
                        'The shareable link has been copied to your clipboard.',
                        2500
                    )
                }).catch((error) => {
                    // display url for manual copy if the clipboard fails
                    globalThis.alert('Clipboard failure.\nPlease copy manually:\n\n' + full_url);
                });
            } else {
                // display url for manual copy if the clipboard is not available
                globalThis.alert('Clipboard not available.\nPlease copy manually:\n\n' + full_url);
            }
        }
    };
}
