// Product Ingestion job using the chunk-oriented approach:
// https://developer.salesforce.com/docs/commerce/b2c-commerce/guide/b2c-custom-job-steps.html

var ProductMgr = require('dw/catalog/ProductMgr');

var algoliaData;
var AlgoliaLocalizedProduct;
var algoliaIngestionAPI;

var logger;
var logData;

var taskID;
var products;

/**
 * Operation class that represents an Algolia operation
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
 * Any returns from this function result in skipping to the afterStep() function (omitting read-process-writealtogether)
 * with the "success" parameter passed to it set to false.
 * @param {dw.util.HashMap} parameters job step parameters
 * @param {dw.job.JobStepExecution} stepExecution contains information about the job step
 */
exports.beforeStep = function(parameters, stepExecution) {
    var Site = require('dw/system/Site');
    algoliaData = require('*/cartridge/scripts/algolia/lib/algoliaData');
    AlgoliaLocalizedProduct = require('*/cartridge/scripts/algolia/model/algoliaLocalizedProduct');
    algoliaIngestionAPI = require('*/cartridge/scripts/algoliaIngestionAPI');
    logger = require('dw/system/Logger').getLogger('algolia', 'Algolia');

    if (empty(parameters.locale)) {
        let errorMessage = 'Mandatory job step parameter missing!';
        logger.error(errorMessage);
        return;
    }
    var siteLocales = Site.getCurrent().getAllowedLocales();
    if (siteLocales.indexOf(parameters.locale) < 0) {
        logger.error('Locale ' + parameters.locale + ' is not allowed.');
        return;
    }

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

    var indexingConfig = JSON.parse(algoliaData.getPreference("Indexing_Config"));
    if (!indexingConfig) {
        logData.processedErrorMessage = 'Missing Indexing configuration';
        algoliaData.setLogData('LastProductSyncLog', logData);
        return;
    }

    taskID = indexingConfig.locales[parameters.locale].products.tasks.replace;
    if (!taskID) {
        logger.error('Locale "' + parameters.locale + '" is not registered on the Ingestion platform.');
        return;
    }
    logger.info('Task found for locale ' + parameters.locale + '; taskID=' + taskID);

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
 * @param {dw.catalog.Product} product one single product
 * @param {dw.util.HashMap} parameters job step parameters
 * @param {dw.job.JobStepExecution} stepExecution contains information about the job step
 * @returns {AlgoliaOperation} the product object in the form in which it will be sent to Algolia
 */
exports.process = function(product, parameters, stepExecution) {
    var algoliaProduct = new AlgoliaLocalizedProduct(product, parameters.locale);
    var productUpdate = new AlgoliaOperation('addObject', algoliaProduct);

    logData.processedRecords++;

    return productUpdate;
}

/**
 * write-function (steptypes.json)
 * Any returns from this function result in the "success" parameter of "afterStep()" to become false.
 * @param {dw.util.List} algoliaOperations a List containing ${chunkSize} AlgoliaOperation objects
 * @param {dw.util.HashMap} parameters job step parameters
 * @param {dw.job.JobStepExecution} stepExecution contains information about the job step
 */
exports.send = function(algoliaOperations, parameters, stepExecution) {
    var status;
    var algoliaOperationsArray = algoliaOperations.toArray();
    var productCount = algoliaOperationsArray.length;

    status = algoliaIngestionAPI.sendBatch(taskID, algoliaOperationsArray);
    if (status.error) {
        logData.failedChunks++;
        logData.failedRecords += productCount;
        return;
    }

    logData.sentChunks++;
    logData.sentRecords += productCount;
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