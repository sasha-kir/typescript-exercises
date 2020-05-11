import { promises as fs } from 'fs';

interface DatabaseRecord {
    _id: number;
}

type FieldName<T> = keyof T;
type FieldValue<T> = T[FieldName<T>];

type QueryOp = '$eq' | '$gt' | '$lt'
type ArrayOp = '$in'
type SetOp = '$and' | '$or';

type QueryMethod = <V>(a: V, b: V) => boolean
type QueryOperationsObject = { [key in QueryOp]: QueryMethod }
const queryOperations: QueryOperationsObject = {
    '$eq': (a, b) => a === b,
    '$gt': (a, b) => a > b,
    '$lt': (a, b) => a < b,
}

type ArrayMethod = <V>(v: V, arr: V[]) => boolean
type ArrayOperationsObject = { [key in ArrayOp]: ArrayMethod }
const arrayOperations: ArrayOperationsObject = {
    '$in': (v, arr) => arr.includes(v),
}

const simpleOperations: QueryOperationsObject & ArrayOperationsObject = {
    ...queryOperations,
    ...arrayOperations
}

const setOperations: { [key in SetOp]: number } = {
    '$and': 1,
    '$or': 2
}

type OpSubQuery<O, V> = { [key in keyof O]?:  V }
type FieldSubQuery<T> = OpSubQuery<typeof queryOperations, FieldValue<T>>
type FieldQuery<T> = { [key in FieldName<T>]?: FieldSubQuery<T> };

type ArraySubQuery<T> = OpSubQuery<typeof arrayOperations, FieldValue<T>[]>
type ArrayQuery<T> = { [key in FieldName<T>]?:  ArraySubQuery<T> };

type SimpleSubQuery<T> = FieldSubQuery<T> | ArraySubQuery<T>
type SimpleQuery<T> = FieldQuery<T> | ArrayQuery<T>;

type SetQuery<T> = { [key in SetOp]?: FieldQuery<T>[] };
type TextQuery = { $text: string };

export class Database<T extends DatabaseRecord> {
    protected filename: string;
    protected fullTextSearchFieldNames: FieldName<T>[];
    protected records: T[];

    constructor(filename: string, fullTextSearchFieldNames: FieldName<T>[]) {
        this.filename = filename;
        this.fullTextSearchFieldNames = fullTextSearchFieldNames;
        this.records = [];
    }

    async loadDatabase(): Promise<void> {
        const dbContents = await fs.readFile(this.filename, "utf8");
        const data = dbContents.split('\n').reduce((res: T[], row: string) => {
            if (row[0] === 'E') {
                const rowData = JSON.parse(row.substring(1))
                return [ ...res, rowData ];
            } else {
                return res;
            }
        }, []);
        this.records = data;
    }

    uniteResults(finalResult: T[], queryResult: T[]): T[] {
        const currentIds = finalResult.map(entry => entry._id);
        const total = queryResult.reduce((res, entry) => {
            return currentIds.includes(entry._id) ? res : [ ...res, entry ]
        }, finalResult)
        return total
    }

    intersectResults(finalResult: T[], queryResult: T[]): T[] {
        const currentIds = finalResult.map(entry => entry._id);
        let total: T[];
        if (finalResult.length) {
            const commonIds = queryResult.reduce((res: number[], entry) => {
                return currentIds.includes(entry._id) ? [...res, entry._id] : res;
            }, []);
            total = finalResult.filter(entry => commonIds.includes(entry._id));
        } else {
            total = [...queryResult]
        }
        return total;
    }

    handleSimpleQuery(operator: QueryOp, subQuery: FieldSubQuery<T>, field: FieldName<T>): T[]
    handleSimpleQuery(operator: ArrayOp, subQuery: ArraySubQuery<T>, field: FieldName<T>): T[]
    handleSimpleQuery(operator: QueryOp | ArrayOp, subQuery: any, field: FieldName<T>): T[] {
        const valueToCompare = subQuery[operator];
        if (valueToCompare === undefined) throw new Error('something went wrong');
        const filterFn = simpleOperations[operator] as QueryMethod & ArrayMethod;
        return this.records.filter(entry => filterFn<FieldValue<T>>(entry[field], valueToCompare));
    }

    prepareText = (text: string): string[] => text.toLowerCase().split(" ");

    handleTextQuery(searchQuery: string): T[] {
        const preparedQuery = this.prepareText(searchQuery);
        return this.records.filter(entry => {
            const fieldValues = Object.entries(entry)
                .filter(elem => {
                    return this.fullTextSearchFieldNames.includes(elem[0] as FieldName<T>)
                })
                .map(elem => elem[1])
                .join(" ");
            const fieldContent = this.prepareText(fieldValues);
            return fieldContent.some(word => preparedQuery.includes(word));
        })
    }

    handleSetQuery(setOperator: SetOp, queryArray: FieldQuery<T>[]): T[] {
        let result: T[] = [];
        queryArray.forEach(setQuery => {
            Object.entries(setQuery).forEach(innerElem => {
                const [key, value] = innerElem;
                const fieldName = key as FieldName<T>
                const subQuery = value as SimpleSubQuery<T>
                const operator = Object.keys(subQuery)[0];
                const setQueryResult = this.handleSimpleQuery(operator as QueryOp, subQuery as FieldSubQuery<T>, fieldName);
                switch (setOperator) {
                    case "$or":
                        result = this.uniteResults(result, setQueryResult);
                        break;
                    case "$and":
                        result = this.intersectResults(result, setQueryResult);
                        break;
                    default:
                        throw new Error("unhandled set operator")
                }
            })
        });
        return result;
    }

    async find(query: SimpleQuery<T> | SetQuery<T> | TextQuery): Promise<T[]> {
        await this.loadDatabase();
        if (!this.records.length) return [];

        const queryElements = Object.entries(query);
        const [key, value] = queryElements[0];

        if (key === '$text') {
            // query is TextQuery
            return this.handleTextQuery(value as string);
        } else if (key in setOperations) {
            // query is SetQuery
            return this.handleSetQuery(key as SetOp, value as FieldQuery<T>[]);
        } else if (key in this.records[0]) {
            let result: T[] = [];
            for (const elem of queryElements) {
                const [key, value] = elem;
                // query is either FieldQuery or ArrayQuery
                const fieldName = key as FieldName<T>
                const subQuery = value as SimpleSubQuery<T>
                const operator = Object.keys(subQuery)[0];
                if (operator in queryOperations) {
                    // query is FieldQuery
                    result = this.intersectResults(result, this.handleSimpleQuery(operator as QueryOp, subQuery as FieldSubQuery<T>, fieldName));
                } else if (operator in arrayOperations) {
                    // query is ArrayQuery
                    result = this.intersectResults(result, this.handleSimpleQuery(operator as ArrayOp, subQuery as ArraySubQuery<T>, fieldName));
                } else {
                    throw new Error('unhandled query operator');
                }
            }
            return result;
        } else {
            throw new Error('unhandled query format');
        }
    }
}
