var express = require('express');
var router = express.Router();
var homeController = require('../controllers/homeController');
var locController = require('../controllers/locController');

/* GET home page. */
router.get('/', homeController.index);
router.ws('/liveloc',locController.queloc);

module.exports = router;
