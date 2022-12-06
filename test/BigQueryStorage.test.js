/**
 * @author David Menger
 */
'use strict';

// const assert = require('assert');
const { Router, Tester, onInteractionHandler } = require('wingbot');
const BigQueryStorage = require('../src/BigQueryStorage');
const credentials = require('./credentials.json');

describe('EventsStorage', () => {

    /** @type {BigQueryStorage} */
    let bgq;

    /** @type {InstanceType<Tester>} */
    let t;

    beforeEach(() => {
        bgq = new BigQueryStorage(credentials, credentials.project_id, 'sasalele', { throwExceptions: true });

        const bot = new Router();

        bot.use('start', (req, res) => {
            res.text('Hello');
        });

        bot.use((req, res) => {
            res.text('Fallback');
        });

        t = new Tester(bot);

        const {
            onInteraction
        } = onInteractionHandler({
            enabled: true
        }, bgq);

        t.processor.onInteraction(onInteraction);
    });

    it('should track data', async () => {
        await t.postBack('start');

        await t.text('hello');
    });

});
