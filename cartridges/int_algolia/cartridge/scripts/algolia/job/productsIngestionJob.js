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
 * The function returns the filtered next product from SeekableIterator
 * and converted to the Algolia Localized Product Model
 * @param {dw.util.SeekableIterator} productsIterator - Product SeekableIterator
 * @param {string} locale - The locale for which we want to retrieve the products properties
 * @returns {Object} -  Algolia Localized Product Model
 */
function getNextProductModel(productsIterator, locale) {
    var productFilter = require('*/cartridge/scripts/algolia/filters/productFilter');
    var AlgoliaLocalizedProduct = require('*/cartridge/scripts/algolia/model/algoliaLocalizedProduct');
    var algoliaProductModel = null;
    while (productsIterator.hasNext()) {
        var product = productsIterator.next();
        if (productFilter.isInclude(product)) {
            algoliaProductModel = new AlgoliaLocalizedProduct(product, locale);
            break;
        }
    }
    return algoliaProductModel;
}

/**
 * Job to reindex all products using the Algolia Ingestion API
 * @param {Object} parameters - job parameters
 * @returns {dw.system.Status} - successful Job run
 */
function runProductExport(parameters) {
    var Status = require('dw/system/Status');
    var Site = require('dw/system/Site');
    var ProductMgr = require('dw/catalog/ProductMgr');

    var logger = require('dw/system/Logger').getLogger('algolia');
    var jobHelper = require('*/cartridge/scripts/algolia/helper/jobHelper');
    var algoliaData = require('*/cartridge/scripts/algolia/lib/algoliaData');
    var algoliaIngestionAPI = require('*/cartridge/scripts/algoliaIngestionAPI');

    var counterProductsTotal = 0;
    var productLogData = algoliaData.getLogData('LastProductSyncLog');
    productLogData.processedDate = algoliaData.getLocalDateTime(new Date());
    productLogData.processedError = true;
    productLogData.processedErrorMessage = '';
    productLogData.processedRecords = 0;
    productLogData.processedToUpdateRecords = 0;

    var status = new Status(Status.ERROR);
    if (!jobHelper.checkAlgoliaFolder()) {
        jobHelper.logFileError('No folder', 'Unable to create Algolia folder', status);
        return status;
    }

    if (!algoliaData.getPreference('Enable')) {
        jobHelper.logFileError('Disable', 'Algolia Cartridge Disabled', status);
        productLogData.processedErrorMessage = 'Algolia Cartridge Disabled';
        algoliaData.setLogData('LastProductSyncLog', productLogData);
        return status;
    }

    var indexingConfig = JSON.parse(algoliaData.getPreference("Indexing_Config"));
    if (!indexingConfig) {
        jobHelper.logFileError('No Config', 'Missing Indexing configuration', status);
        productLogData.processedErrorMessage = 'Missing Indexing configuration';
        algoliaData.setLogData('LastProductSyncLog', productLogData);
        return status;
    }

    var timings = { getProducts: 0, getProductModel: 0, send: 0 };
    var start = 0;
    var duration = 0;

    // TODO limit to the main locales
    var siteLocales = Site.getCurrent().getAllowedLocales();
    var siteLocalesSize = siteLocales.size();
    for (var l = 0; l < siteLocalesSize; ++l) {
        var locale = siteLocales[l];

        var taskID = indexingConfig.locales[locale].products.tasks.replace;
        if (!taskID) {
            logger.error('Locale "' + locale + '" is not registered on the Ingestion platform. Skipping.');
            continue;
        }
        logger.info('Processing locale ' + locale + '; taskID=' + taskID);

        start = Date.now();
        var productsIterator = ProductMgr.queryAllSiteProductsSorted();
        duration = Date.now() - start;
        timings.getProducts += duration;

        var operations = [];
        var nextProduct = getNextProductModel(productsIterator, locale);
        while (nextProduct) {
            ++counterProductsTotal;

            var productUpdate = new AlgoliaOperation('addObject', nextProduct);

            operations.push(productUpdate);
            if (operations.length === 500) {
                logger.info('Sending batch of 500...');
                start = Date.now();
                algoliaIngestionAPI.sendBatch(taskID, operations);
                duration = Date.now() - start;
                timings.send += duration;
                operations = [];
            }

            start = Date.now();
            nextProduct = getNextProductModel(productsIterator);
            duration = Date.now() - start;
            timings.getProductModel += duration;
        }
        logger.info('Sending last batch of ' + operations.length + '...');
        algoliaIngestionAPI.sendBatch(taskID, operations);
        logger.info('Processed ' + counterProductsTotal + ' records for locale ' + locale);
        logger.info('Timings: ' + JSON.stringify(timings));
        counterProductsTotal = 0;
    }

    productsIterator.close();

    productLogData.processedDate = algoliaData.getLocalDateTime(new Date());
    productLogData.processedError = false;
    productLogData.processedErrorMessage = '';
    productLogData.processedRecords = counterProductsTotal;
    algoliaData.setLogData('LastProductSyncLog', productLogData);

    return new Status(Status.OK);
}

module.exports.execute = runProductExport;
