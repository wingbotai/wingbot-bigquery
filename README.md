# wingbot.ai BigQuery analytics storage

```
npm i wingbot-bigquery
```

## Usage

```javascript
const { BotApp } = require('wingbot');
const { BigQueryStorage } = require('wingbot-bigquery');

const analyticsStorage = new BigQueryStorage(credentials, projectId, datasetName);

const bot = new BotApp(/** bot app config here */);

bot.registerAnalyticsStorage(analyticsStorage);
```

## How to run tests
Add credentials.json to `./test` folder

```
npm run test
```
