import { MongoClient, Db, WithId, ObjectId, } from 'mongodb';
import { default as axios, } from 'axios';

interface Protocol {
    id: string
    name: string
    slug: string
}

interface TVL {
    date: number
    totalLiquidityUSD: number
}

interface TokenValue {
    [key: string]: number
}

interface ProtocolDetail {
    id: string
    name: string
    chainTvls: {
        [key: string]: {
            tvl: Array<TVL>
            tokensInUsd: Array<{
                date: number
                tokens: TokenValue
            }>
            tokens: Array<{
                date: number
                tokens: TokenValue
            }>
        }
    }
    currentChainTvls: TokenValue
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

    async getProtocolDetail(slug: string): Promise<ProtocolDetail> {
        return this.client.get('/protocol/' + slug).then(res => {
            console.log(slug, res.status);
            return res.data as ProtocolDetail;
        });
    }

}

type JobUpdateHistory = {
    id: string,
    histories: {
        id: string,
        updatedAt: string,
    }[],
    lastUpdatedIndex: number,
    lastRunAt: string,
};

class MongoStorage {
    private readonly mongo: MongoClient;
    private readonly db: Db;

    constructor(mongoClient: MongoClient) {
        this.mongo = mongoClient;
        this.db = mongoClient.db('defillama');
    }

    async initialize(): Promise<void> {
        await this.mongo.connect();
        await this.db.collection<Protocol>('protocols').createIndex({ id: 1, }, { unique: true, });
        await this.db.collection<Protocol>('protocol_tvls').createIndex({ slug: 1, chain: 1, date: 1, }, { unique: true, });
        await this.db.collection<Protocol>('protocol_tokens').createIndex({ slug: 1, chain: 1, token: 1, date: 1 }, { unique: true, });
        await this.db.collection<Protocol>('protocol_tokens_in_usd').createIndex({ slug: 1, chain: 1, token: 1, date: 1, }, { unique: true, });
    }

    async getProtocolUpdateHistory(): Promise<JobUpdateHistory> {
        const collection = this.db.collection('metadata');
        return (await collection.findOne<JobUpdateHistory>({ id: 'protocol_run_history' })) || {
            id: 'protocol_run_history',
            histories: [],
            lastUpdatedIndex: 0,
            lastRunAt: 0,
        };
    }

    async updateProtocolUpdateHistory(history: JobUpdateHistory): Promise<void> {
        const collection = this.db.collection('metadata');
        await collection.deleteOne({ id: 'protocol_run_history' })
        await collection.insertOne(history);
    }

    async updateProtocol(protocol: Protocol): Promise<void> {
        const collection = this.db.collection<Protocol>('protocols');
        await collection.deleteOne({ id: protocol.id })
        await collection.insertOne(protocol);
    }

    async updateChainTvls(slug: string, chainTvls: {
        [key: string]: {
            tvl: Array<TVL>
            tokensInUsd: Array<{
                date: number
                tokens: TokenValue
            }>
            tokens: Array<{
                date: number
                tokens: TokenValue
            }>
        }
    }): Promise<void> {
        {
            const tvlCollection = this.db.collection('protocol_tvls');
            const records = Object.keys(chainTvls)
                .flatMap(chain => chainTvls[chain].tvl
                    .map(tvl => ({
                        slug,
                        chain,
                        date: tvl.date,
                        totalLiquidityUSD: tvl.totalLiquidityUSD,
                    })));
            if (records.length > 0) {
                await tvlCollection.deleteMany({ slug, });
                await tvlCollection.insertMany(records);
            }
        }
        {
            const tokenCollection = this.db.collection('protocol_tokens');
            const records = Object.keys(chainTvls)
                .flatMap(chain => {
                    const usdByDateByToken = new Map<string, Map<number, number>>();
                    chainTvls[chain].tokensInUsd.forEach(({ date, tokens }) => {
                        Object.keys(tokens).forEach(t => {
                            const usdByDate = usdByDateByToken.get(t) || new Map<number, number>();
                            if (!usdByDateByToken.has(t)) {
                                usdByDateByToken.set(t, usdByDate);
                            }
                            usdByDate.set(date, tokens[t]);
                        });
                    });
                    return chainTvls[chain].tokens.flatMap(({ date, tokens }) => Object.keys(tokens)
                        .map(t => ({
                            slug,
                            chain,
                            token: t,
                            date,
                            amount: tokens[t],
                            amountInUsd: usdByDateByToken.get(t)?.get(date) || 0
                        })))
                });
            if (records.length > 0) {
                await tokenCollection.deleteMany({ slug, });
                await tokenCollection.insertMany(records);
            }
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
            mongoStorage: mongoStorage,
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
        mongoStorage,
        shutdown,
    } = await init();

    const update = async () => {
        try {
            const protocolUpdateHistory = await mongoStorage.getProtocolUpdateHistory();
            const protocolSummaries = await defiLlamaClient.getAllProtocols();
            const summaryByMap = new Map<string, Protocol>();
            protocolSummaries.forEach(p => {
                // https://www.mongodb.com/community/forums/t/just-to-point-out-do-not-name-a-field-language-if-you-are-planning-to-create-an-index/263793/2
                delete p['language'];
                summaryByMap.set(p.slug, p);
            });


            if (protocolSummaries.length !== protocolUpdateHistory.histories.length) {
                const ids = new Set(protocolUpdateHistory.histories.map(h => h.id));
                protocolSummaries.forEach(p => !ids.has(p.slug) && protocolUpdateHistory.histories.push({
                    id: p.slug,
                    updatedAt: '2000-01-1T00:00:00.000Z'
                }));
            }

            for (let i = protocolUpdateHistory.lastUpdatedIndex % protocolUpdateHistory.histories.length; i < protocolUpdateHistory.histories.length; i++) {
                const history = protocolUpdateHistory.histories[i];
                const updateTime = new Date(Date.now() - 24 * 60 * 60 * 1000).toUTCString();
                if (history.updatedAt > updateTime) {
                    continue;
                }
                const detail = await defiLlamaClient.getProtocolDetail(history.id);
                const protocol = summaryByMap.get(history.id);
                protocol.currentChainTvls = detail.currentChainTvls;
                await mongoStorage.updateChainTvls(history.id, detail.chainTvls);
                protocolUpdateHistory.lastUpdatedIndex = i;
                protocolUpdateHistory.lastRunAt = history.updatedAt = new Date().toUTCString();
                await mongoStorage.updateProtocolUpdateHistory(protocolUpdateHistory);
            }
            return true;
        } catch (e) {
            console.error(e, 'reruning...');
            return false;
        }
    }

    while (!(await update()));

    await shutdown();
};

run();




