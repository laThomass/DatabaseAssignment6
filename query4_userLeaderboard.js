import { MongoClient } from "mongodb";
import { createClient } from "redis";

const client = await MongoClient.connect("mongodb://localhost:27017/");
const tweetsCollection = client.db("ieeevisTweets").collection("tweets");

const clientRedis = await createClient();
clientRedis.on("error", (err) => console.log("Redis Client Error", err));
await clientRedis.connect();

await clientRedis.del("leaderboard");

//gets leaderboard with top 10 users with most tweets

const tweets = await tweetsCollection.find({}).toArray();
for (const tweet of tweets) {
    if (tweet.user && tweet.user.screen_name) {
        await clientRedis.zIncrBy("leaderboard", 1, tweet.user.screen_name);
    }
}

const fullLeaderboard = await clientRedis.zRange("leaderboard", 0, -1, {WITHSCORES: true});

const topUsers = await clientRedis.zRange("leaderboard", 0, 9, {
    REV: true,
    WITHSCORES: true
});

console.log("Top 10 Users by Tweets:");
for (let i = 0; i < topUsers.length; i += 2) {
    console.log(`${topUsers[i]}\n${topUsers[i+1]}`); 
}


await clientRedis.disconnect();
await client.close();
