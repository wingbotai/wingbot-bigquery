/**
 * @author David Menger
 */
'use strict';

const { TrackingType } = require('wingbot');
const BaseBigQueryStorage = require('./BaseBigQueryStorage');

/** @typedef {import('@google-cloud/bigquery').BigQueryOptions['credentials']} Credentials */
/** @typedef {import('@google-cloud/bigquery').TableMetadata} TableMetadata */
/** @typedef {import('@google-cloud/bigquery').TableSchema} TableSchema */

/** @typedef {import('wingbot/src/analytics/onInteractionHandler').IAnalyticsStorage} IAnalyticsStorage */ // eslint-disable-line max-len
/** @typedef {import('wingbot/src/analytics/onInteractionHandler').SessionMetadata} SessionMetadata */ // eslint-disable-line max-len
/** @typedef {import('wingbot/src/analytics/onInteractionHandler').GAUser} GAUser */
/** @typedef {import('wingbot/src/analytics/onInteractionHandler').TrackingEvent} Event */
/** @typedef {import('./BaseBigQueryStorage').Logger} Logger */

/* eslint object-curly-newline: 0 */

/** @type {TableSchema} */
const EVENTS_SCHEMA = {
    fields: [
        { name: 'pageId', type: 'STRING', maxLength: '36', mode: 'REQUIRED' }, // uuid length
        { name: 'senderId', type: 'STRING', maxLength: '36', mode: 'REQUIRED' },
        { name: 'sessionId', type: 'STRING', maxLength: '32', mode: 'REQUIRED' },

        { name: 'timestamp', type: 'TIMESTAMP', mode: 'REQUIRED' },
        { name: 'datetime', type: 'DATETIME', mode: 'REQUIRED' },
        { name: 'date', type: 'DATE', mode: 'REQUIRED' },

        { name: 'category', type: 'STRING', maxLength: '3', mode: 'REQUIRED' },

        { name: 'type', type: 'STRING', maxLength: '12', mode: 'REQUIRED' },
        { name: 'action', type: 'STRING' },
        { name: 'label', type: 'STRING' },
        { name: 'value', type: 'INTEGER' },
        { name: 'lang', type: 'STRING', maxLength: '2' },
        { name: 'pageCategory', type: 'STRING' },

        { name: 'nonInteractive', type: 'BOOLEAN', mode: 'REQUIRED' }
    ]
};

/** @type {TableSchema} */
const SESSIONS_SCHEMA = {
    fields: [
        { name: 'pageId', type: 'STRING', maxLength: '36', mode: 'REQUIRED' }, // uuid length
        { name: 'senderId', type: 'STRING', maxLength: '36', mode: 'REQUIRED' },
        { name: 'sessionId', type: 'STRING', maxLength: '32', mode: 'REQUIRED' },

        { name: 'sessionStart', type: 'TIMESTAMP', mode: 'REQUIRED' },
        { name: 'sessionStartDate', type: 'DATETIME', mode: 'REQUIRED' },
        { name: 'date', type: 'DATE', mode: 'REQUIRED' },

        { name: 'action', type: 'STRING' },
        { name: 'sessionCount', type: 'INTEGER' },
        { name: 'lang', type: 'STRING', maxLength: '2' },
        { name: 'nonInteractive', type: 'BOOLEAN', mode: 'REQUIRED' },

        { name: 'didHandover', type: 'BOOLEAN', mode: 'REQUIRED' },
        { name: 'feedback', type: 'INTEGER' },

        { name: 'botId', type: 'STRING', maxLength: '36' }, // uuid length
        { name: 'snapshot', type: 'STRING', maxLength: '14' },

        { name: 'browserName', type: 'STRING' },
        { name: 'osName', type: 'STRING' },
        { name: 'deviceType', type: 'STRING' }
    ]
};

/** @type {TableSchema} */
const PAGE_VIEWS_SCHEMA = {
    fields: [
        { name: 'pageId', type: 'STRING', maxLength: '36', mode: 'REQUIRED' }, // uuid length
        { name: 'senderId', type: 'STRING', maxLength: '36', mode: 'REQUIRED' },
        { name: 'sessionId', type: 'STRING', maxLength: '32', mode: 'REQUIRED' },

        { name: 'sessionStart', type: 'TIMESTAMP', mode: 'REQUIRED' },
        { name: 'sessionStartDate', type: 'DATETIME', mode: 'REQUIRED' },
        { name: 'sessionDuration', type: 'INTEGER', mode: 'REQUIRED' },

        { name: 'timestamp', type: 'TIMESTAMP', mode: 'REQUIRED' },
        { name: 'datetime', type: 'DATETIME', mode: 'REQUIRED' },
        { name: 'date', type: 'DATE', mode: 'REQUIRED' },

        { name: 'action', type: 'STRING' },
        { name: 'lastAction', type: 'STRING' },
        { name: 'prevAction', type: 'STRING' },
        { name: 'allActions', type: 'STRING', mode: 'REPEATED' },
        { name: 'skill', type: 'STRING' },

        { name: 'label', type: 'STRING' },
        { name: 'value', type: 'INTEGER' },

        { name: 'lang', type: 'STRING', maxLength: '2' },
        { name: 'isGoto', type: 'BOOLEAN', mode: 'REQUIRED' },
        { name: 'withUser', type: 'BOOLEAN', mode: 'REQUIRED' },
        { name: 'nonInteractive', type: 'BOOLEAN', mode: 'REQUIRED' },

        { name: 'botId', type: 'STRING', maxLength: '36' }, // uuid length
        { name: 'snapshot', type: 'STRING', maxLength: '14' }
    ]
};

/** @type {TableSchema} */
const CONVERSATIONS_SCHEMA = {
    fields: [
        { name: 'pageId', type: 'STRING', maxLength: '36', mode: 'REQUIRED' }, // uuid length
        { name: 'senderId', type: 'STRING', maxLength: '36', mode: 'REQUIRED' },
        { name: 'sessionId', type: 'STRING', maxLength: '32', mode: 'REQUIRED' },

        { name: 'timestamp', type: 'TIMESTAMP', mode: 'REQUIRED' },
        { name: 'datetime', type: 'DATETIME', mode: 'REQUIRED' },
        { name: 'date', type: 'DATE', mode: 'REQUIRED' },

        { name: 'category', type: 'STRING', maxLength: '3', mode: 'REQUIRED' },

        { name: 'action', type: 'STRING' },
        { name: 'lastAction', type: 'STRING' },
        { name: 'label', type: 'STRING' },
        { name: 'value', type: 'INTEGER' },
        { name: 'lang', type: 'STRING', maxLength: '2' },
        { name: 'skill', type: 'STRING' },
        { name: 'prevSkill', type: 'STRING' },
        { name: 'pathname', type: 'STRING' },

        { name: 'text', type: 'STRING' },

        { name: 'expected', type: 'STRING' },

        { name: 'expectedTaken', type: 'BOOLEAN', mode: 'REQUIRED' },
        { name: 'isContextUpdate', type: 'BOOLEAN', mode: 'REQUIRED' },
        { name: 'isAttachment', type: 'BOOLEAN', mode: 'REQUIRED' },
        { name: 'isNotification', type: 'BOOLEAN', mode: 'REQUIRED' },
        { name: 'isQuickReply', type: 'BOOLEAN', mode: 'REQUIRED' },
        { name: 'isPassThread', type: 'BOOLEAN', mode: 'REQUIRED' },
        { name: 'isText', type: 'BOOLEAN', mode: 'REQUIRED' },
        { name: 'isPostback', type: 'BOOLEAN', mode: 'REQUIRED' },
        { name: 'didHandover', type: 'BOOLEAN', mode: 'REQUIRED' },

        { name: 'withUser', type: 'BOOLEAN', mode: 'REQUIRED' },
        { name: 'userId', type: 'STRING' },
        { name: 'feedback', type: 'INTEGER' },

        { name: 'sessionStart', type: 'TIMESTAMP', mode: 'REQUIRED' },
        { name: 'sessionStartDate', type: 'DATETIME', mode: 'REQUIRED' },
        { name: 'sessionDuration', type: 'INTEGER', mode: 'REQUIRED' },

        { name: 'winnerAction', type: 'STRING' },
        { name: 'winnerIntent', type: 'STRING' },
        { name: 'winnerEntities', type: 'STRING', mode: 'REPEATED' },
        { name: 'winnerScore', type: 'FLOAT' },
        { name: 'winnerTaken', type: 'BOOLEAN' },

        { name: 'intent', type: 'STRING' },
        { name: 'intentScore', type: 'FLOAT' },
        { name: 'entities', type: 'STRING', mode: 'REPEATED' },

        { name: 'allActions', type: 'STRING', mode: 'REPEATED' },
        { name: 'pagePath', type: 'STRING' },
        { name: 'pageCategory', type: 'STRING' },

        { name: 'nonInteractive', type: 'BOOLEAN', mode: 'REQUIRED' },

        { name: 'botId', type: 'STRING', maxLength: '36' }, // uuid length
        { name: 'snapshot', type: 'STRING', maxLength: '14' },

        { name: 'sessionCount', type: 'INTEGER' },
        { name: 'responseTexts', type: 'STRING', mode: 'REPEATED' }
    ]
};

/**
 * @class {BigQueryStorage}
 * @implements {IAnalyticsStorage}
 */
class BigQueryStorage extends BaseBigQueryStorage {

    /**
     *
     * @param {Credentials} googleCredentials
     * @param {string} projectId
     * @param {string} dataset
     * @param {object} [options]
     * @param {Logger} [options.log]
     * @param {boolean} [options.throwExceptions]
     * @param {boolean} [options.passiveSchema] - disables automatic topology updates
     */
    constructor (googleCredentials, projectId, dataset, options = {}) {
        const SESSIONS = 'sessions';
        const EVENTS = 'events';
        const CONVERSATIONS = 'conversations';
        const PAGE_VIEWS = 'page_views';
        const VIEW_CONVERSATIONS = 'view_conversations';
        const VIEW_SKILLS = 'view_skills';
        const VIEW_SESSIONS = 'view_sessions';
        const expirationMs = '3118560000000'; // 10 years;
        /** @type {TableMetadata[]} */
        const topology = [
            {
                name: EVENTS,
                schema: EVENTS_SCHEMA,
                timePartitioning: {
                    type: 'DAY', // 'MONTH'
                    expirationMs,
                    field: 'date'
                },
                clustering: {
                    fields: ['pageId', 'sessionId']
                }
            },
            {
                name: CONVERSATIONS,
                schema: CONVERSATIONS_SCHEMA,
                timePartitioning: {
                    type: 'DAY', // 'MONTH'
                    expirationMs,
                    field: 'date'
                },
                clustering: {
                    fields: ['pageId', 'sessionId']
                }
            },
            {
                name: SESSIONS,
                schema: SESSIONS_SCHEMA,
                timePartitioning: {
                    type: 'DAY', // 'MONTH'
                    expirationMs,
                    field: 'date'
                },
                clustering: {
                    fields: ['pageId', 'sessionId']
                }
            },
            {
                name: PAGE_VIEWS,
                schema: PAGE_VIEWS_SCHEMA,
                timePartitioning: {
                    type: 'DAY', // 'MONTH'
                    expirationMs,
                    field: 'date'
                },
                clustering: {
                    fields: ['pageId', 'sessionId']
                }
            },
            {
                name: VIEW_CONVERSATIONS,
                description: CONVERSATIONS,
                timePartitioning: {
                    type: 'DAY', // 'MONTH'
                    expirationMs,
                    field: 'date'
                },
                clustering: {
                    fields: ['pageId']
                },
                materializedView: {
                    enableRefresh: true,
                    // The default value is "1800000" (30 minutes).
                    // refreshIntervalMs: '',
                    /**
                     * [Required] A query whose result is persisted.
                     */
                    query: `SELECT
                        \`date\`,
                        pageId,
                        APPROX_COUNT_DISTINCT (senderId) as senders,
                        APPROX_COUNT_DISTINCT (IF((sessionDuration = 0 AND sessionCount = 1) OR nonInteractive, null, senderId)) as sendersNotEmpty,
                        COUNTIF(sessionDuration = 0 AND sessionCount = 1) as newSendersPrecise,
                        APPROX_COUNT_DISTINCT(sessionId) as sessions,
                        APPROX_COUNT_DISTINCT (IF((sessionDuration = 0 AND sessionCount = 1) OR nonInteractive, null, sessionId)) as sessionsNotEmpty,
                        COUNTIF(sessionDuration = 0) as sessionsPrecise,
                        APPROX_COUNT_DISTINCT (userId) as users,
                        APPROX_COUNT_DISTINCT (IF((sessionDuration = 0 AND sessionCount = 1) OR nonInteractive, null, userId)) as usersNotEmpty,
                        APPROX_COUNT_DISTINCT(IF(withUser, sessionId, null)) as userSessions,
                        COUNT (*) as interactions,
                        COUNTIF (nonInteractive) as nonInteractives,
                        COUNTIF(isQuickReply) as quickReplies,
                        COUNTIF (isText) as texts,
                        COUNTIF(isPostback) as postbacks,
                        COUNTIF (isPassThread) as passThreads, COUNTIF (isAttachment) as attachments,
                        COUNTIF (isContextUpdate) as contextUpdates,
                        APPROX_COUNT_DISTINCT(IF(didHandover, sessionId, null)) as didHandovers,
                        COUNTIF(feedback >= 0) as feedbacks,
                        COUNTIF(feedback = 0) as feedback0,
                        COUNTIF(feedback = 1) as feedback1,
                        COUNTIF(feedback = 2) as feedback2,
                        COUNTIF(feedback = 3) as feedback3,
                        COUNTIF(feedback = 4) as feedback4,
                        COUNTIF(feedback = 5) as feedback5,
                        SUM(IF (feedback >= 0, feedback, 0)) as feedbackSum,
                        COUNTIF (\`value\` = 1) as notHandled,
                        COUNTIF (\`value\` = 0) as handled,
                        COUNT(intentScore) as intentScores,
                        SUM(intentScore) as intentScoreSum
                    FROM \`${projectId}.${dataset}.${CONVERSATIONS}\`
                    GROUP BY 1, 2`
                }
            },
            {
                name: VIEW_SESSIONS,
                description: CONVERSATIONS,
                timePartitioning: {
                    type: 'DAY', // 'MONTH'
                    expirationMs,
                    field: 'date'
                },
                clustering: {
                    fields: ['pageId', 'sessionId']
                },
                materializedView: {
                    enableRefresh: true,
                    query: `SELECT
                        \`date\`,
                        \`pageId\`,
                        \`sessionId\`,
                        ANY_VALUE(senderId) as senderId,
                        ANY_VALUE(sessionStart) as sessionStart,
                        ANY_VALUE(sessionStartDate) as sessionStartDate,
                        MAX(sessionDuration) as sessionDuration,
                        ANY_VALUE(sessionCount) as sessionCount,
                        COUNT (*) as interactions,
                        COUNTIF (nonInteractive) as nonInteractives,
                        COUNTIF(isQuickReply) as quickReplies,
                        COUNTIF (isText) as texts,
                        COUNTIF(isPostback) as postbacks,
                        COUNTIF (isPassThread) as passThreads,
                        COUNTIF (isAttachment) as attachments,
                        COUNTIF (isContextUpdate) as contextUpdates,
                        SUM(\`value\`) as notHandled,
                        LOGICAL_OR(didHandover) as didHandover,
                        COUNTIF(feedback >= 0) as feedbacks,
                        SUM(IF (feedback >= 0, feedback, 0)) as feedbackSum,
                        MAX(feedback) as feedbackMax
                    FROM \`${projectId}.${dataset}.${CONVERSATIONS}\`
                    GROUP BY 1, 2, 3`
                }
            },
            {
                name: VIEW_SKILLS,
                description: CONVERSATIONS,
                timePartitioning: {
                    type: 'DAY', // 'MONTH'
                    expirationMs,
                    field: 'date'
                },
                clustering: {
                    fields: ['pageId']
                },
                materializedView: {
                    enableRefresh: true,
                    query: `SELECT
                        \`date\`,
                        \`pageId\`,
                        \`skill\`,
                        APPROX_COUNT_DISTINCT (senderId) as senders,
                        APPROX_COUNT_DISTINCT (IF((sessionDuration = 0 AND sessionCount = 1) OR nonInteractive, null, senderId)) as sendersNotEmpty,
                        APPROX_COUNT_DISTINCT(sessionId) as sessions,
                        APPROX_COUNT_DISTINCT (IF((sessionDuration = 0 AND sessionCount = 1) OR nonInteractive, null, sessionId)) as sessionsNotEmpty,
                        APPROX_COUNT_DISTINCT (userId) as users,
                        APPROX_COUNT_DISTINCT(IF(withUser, sessionId, null)) as userSessions,
                        COUNT (*) as interactions,
                        COUNTIF (nonInteractive) as nonInteractives,
                        COUNTIF(isQuickReply) as quickReplies,
                        COUNTIF (isText) as texts,
                        COUNTIF(isPostback) as postbacks,
                        COUNTIF (isPassThread) as passThreads,
                        COUNTIF (isAttachment) as attachments,
                        COUNTIF (isContextUpdate) as contextUpdates,
                        SUM(\`value\`) as notHandled,
                        MAX(didHandover) as didHandover,
                        COUNTIF(feedback >= 0) as feedbacks,
                        SUM(IF (feedback >= 0, feedback, 0)) as feedbackSum,
                        MAX(feedback) as feedbackMax
                    FROM \`${projectId}.${dataset}.${CONVERSATIONS}\`
                    GROUP BY 1, 2, 3`
                }
            }
        ];
        super(googleCredentials, projectId, dataset, topology, options);

        this.SESSIONS = SESSIONS;
        this.EVENTS = EVENTS;
        this.CONVERSATIONS = CONVERSATIONS;
        this.PAGE_VIEWS = PAGE_VIEWS;

        this.VIEW_CONVERSATIONS = VIEW_CONVERSATIONS;
        this.VIEW_SKILLS = VIEW_SKILLS;
        this.VIEW_SESSIONS = VIEW_SESSIONS;

    }

    /**
     *
     * @param {string} pageId
     * @param {string} senderId
     * @param {string} sessionId
     * @param {SessionMetadata} [metadata]
     * @param {number} [ts]
     * @param {boolean} [nonInteractive]
     * @returns {Promise}
     */
    async createUserSession (
        pageId,
        senderId,
        sessionId,
        metadata,
        ts,
        nonInteractive = false
    ) {
        const {
            sessionCount,
            botId,
            snapshot,
            lang,
            action,
            didHandover,
            feedback,
            sessionStart,
            timeZone,
            browserName = null,
            osName = null,
            deviceType = null
        } = metadata;

        await this._insert(this.SESSIONS, [
            {
                pageId,
                senderId,
                sessionId,
                sessionStart: this._dateTime(sessionStart),
                sessionStartDate: this._dateTime(sessionStart, timeZone),
                date: this._date(sessionStart, timeZone),
                action,
                sessionCount,
                nonInteractive,
                lang,
                botId,
                snapshot,
                didHandover,
                feedback,
                browserName,
                osName,
                deviceType
            }
        ]);
    }

    _nullable (val) {
        return val || null;
    }

    /**
     *
     * @param {string} pageId
     * @param {string} senderId
     * @param {string} sessionId
     * @param {Event[]} trackingEvents
     * @param {GAUser} [user]
     * @param {number} [ts]
     * @param {boolean} [nonInteractive]
     * @param {boolean} [sessionStarted]
     * @param {SessionMetadata} [metadata]
     * @returns {Promise}
     */
    async storeEvents (
        pageId,
        senderId,
        sessionId,
        trackingEvents,
        user = null,
        ts = Date.now(),
        nonInteractive = false,
        sessionStarted = false, // eslint-disable-line no-unused-vars
        metadata = {}
    ) {
        const {
            botId,
            snapshot,
            // didHandover,
            // feedback,
            sessionCount,
            sessionDuration,
            sessionStart,
            timeZone,
            responseTexts
        } = metadata;

        const conversations = [];
        const pageViews = [];

        const events = trackingEvents
            .filter((e) => {
                if (e.type === TrackingType.CONVERSATION_EVENT && 'isQuickReply' in e) {
                    conversations.push({
                        pageId,
                        senderId,
                        sessionId,
                        timestamp: this._dateTime(ts),
                        datetime: this._dateTime(ts, timeZone),
                        date: this._date(sessionStart, timeZone),
                        category: e.category,
                        action: this._nullable(e.action),
                        lastAction: this._nullable(e.lastAction),
                        label: this._nullable(e.label),
                        value: typeof e.value === 'number' ? e.value : null,
                        lang: this._nullable(e.lang),
                        skill: this._nullable(e.skill),
                        prevSkill: this._nullable(e.prevSkill),
                        pathname: this._nullable(e.pathname),

                        text: this._nullable(e.text),
                        expected: this._nullable(e.expected),

                        expectedTaken: e.expectedTaken,
                        isContextUpdate: e.isContextUpdate,
                        isAttachment: e.isAttachment,
                        isNotification: e.isNotification,
                        isQuickReply: e.isQuickReply,
                        isPassThread: e.isPassThread,
                        isPostback: e.isPostback,
                        isText: e.isText,
                        didHandover: e.didHandover,
                        withUser: e.withUser,

                        userId: this._nullable(user && user.id),
                        feedback: e.feedback,

                        sessionStart: this._dateTime(sessionStart),
                        sessionStartDate: this._dateTime(sessionStart, timeZone),
                        sessionDuration,

                        winnerAction: this._nullable(e.winnerAction),
                        winnerIntent: this._nullable(e.winnerIntent),
                        winnerEntities: e.winnerEntities,
                        winnerScore: e.winnerScore,
                        winnerTaken: e.winnerTaken,

                        intent: this._nullable(e.intent),
                        intentScore: this._nullable(e.intentScore),
                        entities: e.entities,

                        allActions: e.allActions,
                        pagePath: this._nullable(e.pagePath),
                        pageCategory: this._nullable(e.pageCategory),

                        nonInteractive,

                        snapshot: this._nullable(snapshot),
                        botId: this._nullable(botId),

                        sessionCount,
                        responseTexts
                    });
                    return false;
                }
                if (e.type === TrackingType.PAGE_VIEW && 'isGoto' in e) {
                    pageViews.push({
                        pageId,
                        senderId,
                        sessionId,

                        timestamp: this._dateTime(ts),
                        datetime: this._dateTime(ts, timeZone),
                        date: this._date(sessionStart, timeZone),

                        sessionStart: this._dateTime(sessionStart),
                        sessionStartDate: this._dateTime(sessionStart, timeZone),
                        sessionDuration,

                        action: this._nullable(e.action),
                        lastAction: this._nullable(e.lastAction),
                        prevAction: this._nullable(e.prevAction),
                        allActions: e.allActions,
                        skill: this._nullable(e.skill),

                        lang: this._nullable(e.lang),
                        isGoto: e.isGoto,
                        withUser: e.withUser,
                        nonInteractive,

                        label: this._nullable(e.label),
                        value: typeof e.value === 'number' ? e.value : null,

                        snapshot: this._nullable(snapshot),
                        botId: this._nullable(botId)
                    });
                    return false;
                }
                return true;
            })
            .map((e) => ({
                pageId,
                senderId,
                sessionId,
                timestamp: this._dateTime(ts),
                datetime: this._dateTime(ts, timeZone),
                date: this._date(sessionStart, timeZone),
                type: e.type,
                category: e.category,
                action: this._nullable(e.action),
                label: this._nullable(e.label),
                value: typeof e.value === 'number' ? e.value : null,
                lang: this._nullable(e.lang),
                nonInteractive
            }));

        const d = Date.now();
        await Promise.all([
            this._insert(this.CONVERSATIONS, conversations),
            this._insert(this.EVENTS, events),
            this._insert(this.PAGE_VIEWS, pageViews)
        ]);
        if (this._throwExceptions) {
            this._log.log(`BigQueryStorage: inserts\t${Date.now() - d}`);
        }
    }
}

module.exports = BigQueryStorage;
