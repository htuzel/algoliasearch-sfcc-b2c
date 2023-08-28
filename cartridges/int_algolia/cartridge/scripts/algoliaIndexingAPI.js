/**
 *  Client to communicate with Algolia's API:
 *  https://www.algolia.com/doc/rest-api/search/#objects-endpoints
 **/

var algoliaIndexingService = require('*/cartridge/scripts/services/algoliaIndexingService');
var logger = require('dw/system/Logger').getLogger('algolia');

/**
 * Send a batch of objects to Algolia Indexing API
 * @param {string} indexName - name of the index to target
 * @param {Array} requestsArray - array of requests to send to Algolia
 * @returns {dw.system.Status} - successful Status to send
 */
function sendBatch(indexName, requestsArray) {
    var ingestionService = algoliaIndexingService.getService();
    var baseURL = ingestionService.getConfiguration().getCredential().getURL();

    ingestionService.setRequestMethod('POST');
    ingestionService.setURL(baseURL + indexName + '/batch');

    var batchObj = Object.create(null);
    batchObj.requests = requestsArray;

    var callStatus = ingestionService.call(batchObj);

    if (!callStatus.ok) {
        logger.error(callStatus.getErrorMessage());
    }

    return callStatus;
}

module.exports.sendBatch = sendBatch;
