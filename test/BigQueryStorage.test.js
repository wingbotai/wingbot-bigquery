/**
 * @author David Menger
 */
'use strict';

// const assert = require('assert');
const {
    Router, Tester, onInteractionHandler, TrackingType, TrackingCategory
} = require('wingbot');
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
            res.text('Hello').text('World');
            res.trackAsSkill('foo');
        });

        bot.use('feedback', (req, res) => {
            res.trackAsSkill('sasalele');
            res.trackEvent(TrackingType.REPORT, TrackingCategory.REPORT_FEEDBACK, null, null, 10)
                .text('feedback');
        });

        bot.use('handover', (req, res) => {
            res.passThread('abc');
        });

        bot.use((req, res) => {
            res.trackAsSkill('bar');
            res.text('Fallback', [
                { action: 'handover', title: 'do handover' },
                { action: 'feedback', title: 'do feedback' }
            ]);
        });

        t = new Tester(bot);

        const {
            onInteraction
        } = onInteractionHandler({
            enabled: true
        }, bgq);

        t.processor.onInteraction(onInteraction);
    });

    it('should track data with handover', async () => {
        await t.postBack('start');

        await t.text('hello');

        await t.quickReplyText('do handover');
    });

    it('should track data with feedback', async () => {
        await t.postBack('start');

        await t.text('hello');

        await t.quickReplyText('do feedback');
    });

});
