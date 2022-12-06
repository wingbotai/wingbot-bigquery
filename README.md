# Wingbot azure tablestorage

```
npm i wingbot-azure-tablestorage
```

## Usage

```javascript
const { BotApp } = require('wingbot');
const { AnalyticsStorage } = require('wingbot-azure-tablestorage');

const analyticsStorage = new AnalyticsStorage(accountName, accountKey);

const bot = new BotApp(/** bot app config here */);

bot.registerAnalyticsStorage(analyticsStorage);
```

## How to run tests
Create secretKey.json in `./config`

```json
{
    "key":"INSERT_KEY_HERE"
}
```
```
npm run test
```
