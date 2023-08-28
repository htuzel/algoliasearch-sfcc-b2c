// Algolia Indexing job using the chunk-oriented approach:
// https://developer.salesforce.com/docs/commerce/b2c-commerce/guide/b2c-custom-job-steps.html

var ProductMgr = require('dw/catalog/ProductMgr');

var algoliaData;
var AlgoliaLocalizedProduct;
var algoliaIndexingAPI;

var logger;
var logData;

var siteLocales;
var products;
var indexPrefix;

/**
 * Operation class that represents an Algolia batch operation: https://www.algolia.com/doc/rest-api/search/#batch-write-operations
 * @param {string} action - Operation to perform: addObject, updateObject, deleteObject
 * @param {Object} algoliaObject - Algolia object to index
 * @constructor
 */
function AlgoliaOperation(action, algoliaObject) {
    this.action = action;
    this.body = {};

    var keys = Object.keys(algoliaObject);
    for (var i = 0; i < keys.length; i += 1) {
        if (keys[i] !== 'id') {
            this.body[keys[i]] = algoliaObject[keys[i]];
        } else {
            this.body.objectID = algoliaObject.id;
        }
    }
}

/**
 * before-step-function (steptypes.json)
 * @param {dw.util.HashMap} parameters job step parameters
 * @param {dw.job.JobStepExecution} stepExecution contains information about the job step
 */
exports.beforeStep = function(parameters, stepExecution) {
    var Site = require('dw/system/Site');
    algoliaData = require('*/cartridge/scripts/algolia/lib/algoliaData');
    AlgoliaLocalizedProduct = require('*/cartridge/scripts/algolia/model/algoliaLocalizedProduct');
    algoliaIndexingAPI = require('*/cartridge/scripts/algoliaIndexingAPI');
    logger = require('dw/system/Logger').getLogger('algolia', 'Algolia');

    // initializing logs
    logData = algoliaData.getLogData('LastProductSyncLog') || {};
    logData.processedDate = algoliaData.getLocalDateTime(new Date());
    logData.processedError = true;
    logData.processedErrorMessage = '';
    logData.processedRecords = 0;

    logData.sentChunks = 0;
    logData.sentRecords = 0;
    logData.failedChunks = 0;
    logData.failedRecords = 0;

    indexPrefix = algoliaData.getIndexPrefix();
    siteLocales = Site.getCurrent().getAllowedLocales();
    products = ProductMgr.queryAllSiteProducts();
}

/**
 * total-count-function (steptypes.json)
 * @param {dw.util.HashMap} parameters job step parameters
 * @param {dw.job.JobStepExecution} stepExecution contains information about the job step
 * @returns {number} total number of products
 */
exports.getTotalCount = function(parameters, stepExecution) {
    return products.count;
}

/**
 * read-function (steptypes.json)
 * @param {dw.util.HashMap} parameters job step parameters
 * @param {dw.job.JobStepExecution} stepExecution contains information about the job step
 * @returns {dw.catalog.Product} B2C Product object
 */
exports.read = function(parameters, stepExecution) {
    if (products.hasNext()) {
        return products.next();
    }
}

/**
 * process-function (steptypes.json)
 * @param {dw.catalog.Product} product a product
 * @param {dw.util.HashMap} parameters job step parameters
 * @param {dw.job.JobStepExecution} stepExecution contains information about the job step
 * @returns {Object} and object that contains one localized Product per locale:
 *                   { "en_US": { "id": "008884303989M", "name": "Fitted Shirt" },
 *                     "fr_FR": { "id": "008884303989M", "name": "Chemise ajustée" } }
 */
exports.process = function(product, parameters, stepExecution) {
    var localizedProducts = {};
    for (var l = 0; l < siteLocales.size(); ++l) {
        var locale = siteLocales[l];
        var algoliaProduct = new AlgoliaLocalizedProduct(product, locale);
        localizedProducts[locale] = algoliaProduct;
    }
    logData.processedRecords++;

    return localizedProducts;
}

/**
 * write-function (steptypes.json)
 * Any returns from this function result in the "success" parameter of "afterStep()" to become false.
 * @param {dw.util.List} algoliaLocalizedProducts a List containing ${chunkSize} of objects referencing one localized Product per locale
 * @param {dw.util.HashMap} parameters job step parameters
 * @param {dw.job.JobStepExecution} stepExecution contains information about the job step
 */
exports.send = function(algoliaLocalizedProducts, parameters, stepExecution) {
    var status;
    var algoliaLocalizedProductsArray = algoliaLocalizedProducts.toArray();
    var productCount = algoliaLocalizedProductsArray.length;

    for (var l = 0; l < siteLocales.size(); ++l) {
        var locale = siteLocales[l];
        var indexName = indexPrefix + '__products__' + locale;
        var batch = [];
        for (var i = 0; i < productCount; ++i) {
            batch.push(new AlgoliaOperation('addObject', algoliaLocalizedProductsArray[i][locale]));
        }
        logger.info('Sending batch for locale ' + locale + ': ' + indexName);
        status = algoliaIndexingAPI.sendBatch(indexName, batch);
        if (status.error) {
            logData.failedChunks++;
            logData.failedRecords += productCount;
        }
        else {
            logData.sentRecords += productCount;
        }
    }

    logData.sentChunks++;
}

/**
 * after-step-function (steptypes.json)
 * @param {boolean} success any prior return statements and errors will result in this parameter becoming false
 * @param {dw.util.HashMap} parameters job step parameters
 * @param {dw.job.JobStepExecution} stepExecution contains information about the job step
 */
exports.afterStep = function(success, parameters, stepExecution) {
    products.close();

    if (success) {
        logData.processedError = false;
        logData.processedErrorMessage = '';
        logData.sendError = false;
        logData.sendErrorMessage = '';
    } else {
        let errorMessage = 'An error occurred during the job. Please see the error log for more details.';
        logData.processedError = true;
        logData.processedErrorMessage = errorMessage;
        logData.sendError = true;
        logData.sendErrorMessage = errorMessage;
    }

    logData.processedDate = algoliaData.getLocalDateTime(new Date());
    logData.sendDate = algoliaData.getLocalDateTime(new Date());
    algoliaData.setLogData('LastProductSyncLog', logData);

    logger.info('Chunks sent: {0}; Failed chunks: {1}\nRecords sent: {2}; Failed records: {3}',
        logData.sentChunks, logData.failedChunks, logData.sentRecords, logData.failedRecords);
}