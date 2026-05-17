const { join } = require('path');

/**
 * @type {import("puppeteer").Configuration}
 */
module.exports = {
    // Changes the download location to a folder within your repository boundary
    cacheDirectory: join(__dirname, '.cache', 'puppeteer'),
};