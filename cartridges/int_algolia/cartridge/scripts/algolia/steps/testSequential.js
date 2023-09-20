'use strict';

function testSequential(parameters) {

	var ProductMgr = require('dw/catalog/ProductMgr');
	var AlgoliaProduct = require('*/cartridge/scripts/algolia/model/algoliaProduct');
	var Status = require('dw/system/Status');

	var jobHelper = require('*/cartridge/scripts/algolia/helper/jobHelper');
	var sendHelper = require('*/cartridge/scripts/algolia/helper/sendHelper');

	const CHUNK_SIZE = 500;

	let productsIterator = ProductMgr.queryAllSiteProducts();
	let products = productsIterator.asList(0, productsIterator.count).toArray();
	let algoliaProductsArray = [];

	for (let i = 0; i < products.length; i++) {
		let algoliaProduct = new AlgoliaProduct(products[i]);
		let productUpdateObj = new jobHelper.UpdateProductModel(algoliaProduct);

		algoliaProductsArray.push(productUpdateObj);

		if (algoliaProductsArray.length === CHUNK_SIZE || i === (products.length - 1)) {
			sendHelper.sendChunk(algoliaProductsArray);
			algoliaProductsArray = [];
		}
	}

	return new Status(Status.OK);
}

module.exports.testSequential = testSequential;
