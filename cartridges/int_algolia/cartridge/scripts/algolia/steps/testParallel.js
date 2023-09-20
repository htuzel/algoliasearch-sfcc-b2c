var ProductMgr = require('dw/catalog/ProductMgr');

var AlgoliaProduct, jobHelper, sendHelper;

var productsIterator, currentIndex;

exports.beforeStep = function(parameters, stepExecution) {

	AlgoliaProduct = require('*/cartridge/scripts/algolia/model/algoliaProduct');
    jobHelper = require('*/cartridge/scripts/algolia/helper/jobHelper');
	sendHelper = require('*/cartridge/scripts/algolia/helper/sendHelper');

	productsIterator = ProductMgr.queryAllSiteProducts();
	productsIterator = productsIterator.asList(0, productsIterator.count).toArray();
	currentIndex = 0;
}

exports.getTotalCount = function(parameters, stepExecution) {
	return productsIterator.length;
}

exports.read = function(parameters, stepExecution) {

	if (currentIndex < productsIterator.length) {
		return productsIterator[currentIndex++];
	}
	// return products[Math.floor(Math.random() * products.length)];
}

exports.process = function(product, parameters, stepExecution) {
	let algoliaProduct = new AlgoliaProduct(product);
	let productUpdateObj = new jobHelper.UpdateProductModel(algoliaProduct);
	return productUpdateObj;
}

exports.send = function(algoliaProductsArray, parameters, stepExecution) {
	sendHelper.sendChunk(algoliaProductsArray, 'product');
}
