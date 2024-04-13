import { MongoClient } from "mongodb";
import { createClient } from "redis";

// Connect to MongoDB
const client = await MongoClient.connect("mongodb://localhost:27017/");
const tweetsCollection = client.db("ieeevisTweets").collection("tweets");

const clientRedis = await createClient();
clientRedis.on("error", (err) => console.log("Redis Client Error", err));
await clientRedis.connect();

//creates custom structure that gets you all tweets for specific user
try {
    const tweets = await tweetsCollection.find({}).toArray();

    for (const tweet of tweets) {
        if (tweet.user && tweet.user.screen_name) {
            const listKey = `user_tweets:${tweet.user.screen_name}`;
            const hashKey = `tweet:${tweet.id_str}`; 

            await clientRedis.rPush(listKey, tweet.id_str);

            await clientRedis.hSet(hashKey, {
                'user_name': tweet.user.screen_name,
                'text': tweet.text,
                'created_at': tweet.created_at
            });
        }
    }

    console.log("Data structure for user tweets has been populated in Redis.");
} finally {
    await clientRedis.disconnect();
    await client.close();
}

