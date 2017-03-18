var express = require('express');
var router = express.Router();
var homeController = require('../controllers/homeController');
var carController = require('../controllers/carController');

/* GET home page. */
router.get('/', homeController.index);
router.post('/getCar', carController.getCar);

module.exports = router;
