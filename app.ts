import { MongoClient, Db, WithId, ObjectId, } from 'mongodb';
import { default as axios, } from 'axios';

interface ChainTvls {
    [key: string]: number,
}

interface TokenBreakdowns { }

interface Protocol {
    id: string
    name: string
    address: any
    symbol: string
    url: string
    description: string
    chain: string
    logo: string
    audits: string
    audit_note: any
    gecko_id: any
    cmcId: any
    category: string
    chains: string[]
    module: string
    twitter: string
    forkedFrom: any[]
    oracles: any[]
    listedAt: number
    methodology: string
    slug: string
    tvl: number
    chainTvls: ChainTvls
    change_1h: number
    change_1d: number
    change_7d: number
    tokenBreakdowns: TokenBreakdowns
    mcap: any
}

class DefiLlamaClient {
    private readonly client: Axios.AxiosInstance;

    constructor() {
        this.client = axios.create({
            baseURL: 'https://api.llama.fi'
        });
    }

    async getAllProtocols(): Promise<Protocol[]> {
        return this.client.get('/protocols').then(res => res.data as Protocol[]);
    }

}

class MongoStorage {
    private readonly mongo: MongoClient;
    private readonly db: Db;

    constructor(mongoClient: MongoClient) {
        this.mongo = mongoClient;
        this.db = mongoClient.db('defillama');
    }

    async initialize(): Promise<void> {
        await this.mongo.connect();
        await this.db.collection<Protocol>('protocols').createIndex({ id: 'text', }, { unique: true, });
    }

    async updateProtocols(protocols: Protocol[]): Promise<void> {
        const collection = this.db.collection<Protocol>('protocols');
        const storedProtocols = await collection.find({}).toArray();
        const ids = new Set(storedProtocols.map(p => p.id));
        const toInsert: Protocol[] = [];

        // https://www.mongodb.com/community/forums/t/just-to-point-out-do-not-name-a-field-language-if-you-are-planning-to-create-an-index/263793/2
        for (let p of protocols) {
            delete p['language'];
        }
        for (let p of protocols) {
            if (!ids.has(p.id)) {
                toInsert.push(p);
            }
        }
        if (toInsert.length > 0) {
            await collection.insertMany(toInsert);
        }
        const updates: any[] = [];
        for (let p of protocols) {
            if (ids.has(p.id)) {
                updates.push({
                    replaceOne: {
                        filter: { id: p.id, },
                        replacement: p,
                    }
                });
            }
        };
        if (updates.length > 0) {
            await collection.bulkWrite(updates);
        }
    }
}


const init = async () => {
    try {
        const mongo = new MongoClient('mongodb://localhost:27017');
        const mongoStorage = new MongoStorage(mongo);
        await mongoStorage.initialize();
        return {
            defiLlamaClient: new DefiLlamaClient(),
            mongo: new MongoStorage(mongo),
            shutdown: async () => {
                await mongo.close();
                console.log("resources shut down successfully...");
            },
        };
    } catch (e) {
        console.log(e, 'process exited...');
        process.exit(-1);
    }
};




const run = async () => {
    const {
        defiLlamaClient,
        mongo,
        shutdown,
    } = await init();

    const protocols = await defiLlamaClient.getAllProtocols();
    await mongo.updateProtocols(protocols);

    await shutdown();
};

run();



