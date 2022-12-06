/**
 * @author David Menger
 */
 'use strict';

 module.exports = {
     diff: true,
     extension: ['js'],
     package: '../package.json',
     reporter: 'spec',
     timeout: 60000,
     ui: 'bdd',
     recursive: true,
     spec: ["./test/**/*.test.js"],
 };
 