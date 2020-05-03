import * as fs from 'fs';
import * as AsyncLock from 'async-lock';
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
type SortOption<T> = { [key in keyof T]?: -1 | 1 }; // Or Partial<Record<keyof  T, -1|1>>;
type ProjectionOption<T> = { [key in keyof T]?: 1 }
type QueryOptions<T> = { sort?: SortOption<T>; projection?: ProjectionOption<T>, a?: string };

type KeysMatching<T, V> = { [K in keyof T]: T[K] extends V ? K : never }[keyof T];
export class Database<T> {
    protected filename: string;
    protected fullTextSearchFieldNames: KeysMatching<T, string>[];
    private lock = new AsyncLock();
    private lockKey = 'lock';

    constructor(filename: string, fullTextSearchFieldNames: KeysMatching<T, string>[]) {
        this.filename = filename;
        this.fullTextSearchFieldNames = fullTextSearchFieldNames;
    }

    async find(query: Query<T>, options?: QueryOptions<T>): Promise<Partial<T>[]> {
        return await this.lock.acquire(this.lockKey, async () => {
            let records = await this.getRecords();
            records = records.filter(x => this.matchesQuery(x, query));
            if (options?.sort) {
                this.sort(records, options.sort);
            }
            if (options?.projection) {
                let projection = options.projection;
                return records.map(x => this.project(x, projection));
            }
            return records;
        })
    }

    async insert(record: T, asDeleted = false) {
        const symbol = asDeleted ? 'D' : 'E';
        await this.lock.acquire(this.lockKey, () => new Promise(resolve =>
            fs.appendFile(this.filename, `${symbol}${JSON.stringify(record)}\n`, { encoding: 'utf8' }, () => resolve())));
    }

    async delete(query: Query<T>) {
        await this.lock.acquire(this.lockKey, async() => {
            const records = await this.getRecords();
            let recordsToDelete = records.filter(x => this.matchesQuery(x, query));
            await new Promise(resolve => fs.truncate(this.filename, 0, () => resolve()))
            records.forEach(async r => await this.insert(r, recordsToDelete.includes(r)));
        })
    }

    private project(record: T, projectionOption: ProjectionOption<T>): Partial<T> {
        let result: Partial<T> = {};
        for (let key in projectionOption) {
            result[key] = record[key];
        }
        return result;
    }

    private sort(records: T[], sortOption: SortOption<T>) {
        records = records.sort((a, b) => {
            for (let key in sortOption) {
                if (a[key] < b[key]) {
                    return -sortOption[key]!;
                } else if (a[key] > b[key]) {
                    return sortOption[key]!;
                }
            }
            return 0;
        });
    }

    private matchesQuery(record: T, query: Query<T>): boolean {
        if ('$and' in query) {
            return query.$and.every(q => this.matchesQuery(record, q));
        } else if ('$or' in query) {
            return query.$or.some(q => this.matchesQuery(record, q));
        } else if ('$text' in query) {
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
        if (!condition) {
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
            fs.readFile(this.filename, { encoding: 'utf8' }, (error, data) => {
                if (error) {
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
