import express = require("express");
import redis = require("redis");
import uuid = require("uuid");
import crypto = require("crypto");
import process = require("process")


const app = express();
app.use(express.json())

const redis_host = process.env.REDIS_HOST || 'localhost';
const redis_port = process.env.REDIS_PORT || 6379;

const client = redis.createClient({
    url: `redis://${redis_host}:${redis_port}`
});

client.on('error', (err) => console.error('Redis Client Error:', err));
client.connect();

function auth(req:express.Request, res:express.Response, next:express.NextFunction){
    const headers = req.headers;
    const MASTER_API_KEY = process.env.MASTER_API_KEY || "super-secret-key-42";
    const HASHING_SALT = process.env.HASHING_SALT || 'b1a2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2';
    const api_key = headers['authorization'] as string;
    
    if (!MASTER_API_KEY || !HASHING_SALT) {
        throw new Error("Missing required environment variables: MASTER_API_KEY or HASHING_SALT");
    }

    if(api_key===MASTER_API_KEY){
        // attach userId to req
        // currently we are using hash of the api key
        // but IRL we will validate api key with DB and get userId assigned in DB
        const hmac = crypto.createHmac('sha256',HASHING_SALT);
        hmac.update(api_key);
        req.userId = hmac.digest('hex');
        next();
    }else{
        return res.status(403).json({message: "Invalid API Key"})
    }
}

app.use(auth);

app.post('/submit',async (req,res)=>{
    // attaching jobId to req
    req.jobId = uuid.v4()

    try{
        const data = req.body;
        const userId = req.userId;
        const jobId = req.jobId;
        const s3Address = data.s3Address;
        
        console.log(`user id ${userId}`);
        console.log(`job id ${jobId}`);
        console.log(`s3 address ${s3Address}`);
        try{
            await client.lPush("workQueue", JSON.stringify({userId, jobId, s3Address}));
            res.send({taskId: jobId, path : `${userId}/${jobId}.pt`});
            console.log("Successfully added to queue!!");
        }catch(e){
            console.log(e);
        }
    }catch(err){
        console.log(err);
        res.status(400).send("Bad request");
    }

})

app.listen(8081, ()=>{
    console.log("Server is listen at port 8081")
});