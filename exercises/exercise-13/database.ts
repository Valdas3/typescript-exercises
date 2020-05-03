import * as fs from 'fs';
type EqCondition<T> = { $eq: T };
type GtCondition<T> = { $gt: T };
type LtCondition<T> = { $lt: T };
type InCondition<T> = { $in: T[] };
type Condition<T> = EqCondition<T> | GtCondition<T> | LtCondition<T> | InCondition<T>;
type PropertyQuery<T> = { [key in keyof T]?: Condition<T[key]> }; // Or Partial<Record<keyof T, Condition<T[keyof T]>>>
type AndQuery<T> = { $and: Query<T>[] };
type OrQuery<T> = { $or: Query<T>[] };
type TextQuery = { $text: string };
type Query<T> = PropertyQuery<T> | AndQuery<T> | OrQuery<T> | TextQuery;

type KeysMatching<T, V> = {[K in keyof T]: T[K] extends V ? K : never}[keyof T];
export class Database<T> {
    protected filename: string;
    protected fullTextSearchFieldNames: KeysMatching<T, string>[];

    constructor(filename: string, fullTextSearchFieldNames: KeysMatching<T, string>[]) {
        this.filename = filename;
        this.fullTextSearchFieldNames = fullTextSearchFieldNames;
    }

    async find(query: Query<T>): Promise<T[]> {
        const records = await this.getRecords();
        return records.filter(x => this.matchesQuery(x, query));
    }

    private matchesQuery(record: T, query: Query<T>): boolean {
        if('$and' in query) {
            return query.$and.every(q => this.matchesQuery(record, q));
        } else if('$or' in query) {
            return query.$or.some(q => this.matchesQuery(record, q));
        } else if('$text' in query){
            let searchableFieldWords = this.fullTextSearchFieldNames
                .map(key => record[key] as any as string)
                .map(s => s.toLowerCase().split(' '));
            let wordsToSearch = query.$text.toLowerCase().split(' ');
            return wordsToSearch.every(x => searchableFieldWords.some(y => y.includes(x)))
        } else {
            return this.matchesPropertyQuery(record, query);
        }
    }

    private matchesPropertyQuery(record: T, propertyQuery: PropertyQuery<T>): boolean {
        return this.getObjectKeys(propertyQuery).every(key => this.matchesCondition(record[key], propertyQuery[key]))
    }
    
    private matchesCondition(value: T[keyof T], condition: Condition<T[keyof T]> | undefined) {
        if(!condition) {
            return false;
        } else if ('$eq' in condition) {
            return value === condition.$eq;
        } else if ('$gt' in condition) {
            return value > condition.$gt;
        } else if ('$lt' in condition) {
            return value < condition.$lt;
        } else if ('$in' in condition) {
            return condition.$in.includes(value);
        }
    }

    private getObjectKeys<U>(x: U): (keyof U)[] {
        return Object.keys(x) as (keyof U)[];
    }

    private getRecords(): Promise<T[]> {
        return new Promise((resolve, reject) => {
            fs.readFile(this.filename, { encoding: 'utf8'}, (error, data) => {
                if(error) {
                    reject(error)
                } else {
                    const records = data.toString()
                        .trim()
                        .split('\n')
                        .filter(x => x[0] === 'E')
                        .map(x => x.substr(1))
                        .map(x => JSON.parse(x) as T);
                    resolve(records);
                }
            })
        })
    }
}
